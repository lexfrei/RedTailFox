// Package worker implements the RedTailFox container worker process.
package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/Dark-F0X/RedTailFox/internal/model"
)

const (
	tickInterval    = 5 * time.Second
	errorBackoff    = 30 * time.Second
	defaultInterval = 900
	stopTimeout     = 30 * time.Second
)

// SlotInfo provides read-only context about the slot to the work function.
type SlotInfo struct {
	// ID is the unique slot identifier.
	ID int

	// Config is the raw JSON configuration for this slot.
	Config json.RawMessage

	// SetStatus updates the slot status visible in heartbeats.
	SetStatus func(model.SlotStatus)
}

// WorkFunc is the business logic executed by a slot on each work cycle.
// It receives a cancellable context and slot metadata.
// Returning an error logs the failure and triggers an error backoff.
//
// CONTRACT: Implementations MUST respect context cancellation and return
// promptly when ctx.Done() is closed. Failure to do so will cause Slot.Stop()
// to time out and abandon the goroutine, leaking resources.
type WorkFunc func(ctx context.Context, info SlotInfo) error

// Slot represents a single work unit running inside a container.
// Mutable fields (running, status, lastActive) are unexported and accessed
// only through thread-safe methods: IsRunning(), SetStatus(), Snapshot().
type Slot struct {
	ID     int
	Config json.RawMessage

	running    bool
	status     model.SlotStatus
	lastActive int64
	work       WorkFunc
	mu         sync.Mutex
	stopCh     chan struct{}
	doneCh     chan struct{}
	stopOnce   sync.Once
	doneOnce   sync.Once
	started    bool
	log        *slog.Logger
}

// NewSlot creates a new slot with the given ID, config, and work function.
// If work is nil, the slot idles without performing any task.
func NewSlot(slotID int, config json.RawMessage, work WorkFunc, log *slog.Logger) *Slot {
	return &Slot{
		ID:         slotID,
		Config:     config,
		status:     model.SlotStatusIdle,
		lastActive: time.Now().Unix(),
		work:       work,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		log:        log,
	}
}

// Start begins the slot's work loop in a goroutine.
func (s *Slot) Start(ctx context.Context) {
	s.mu.Lock()
	s.running = true
	s.started = true
	s.mu.Unlock()

	go s.run(ctx)
}

// Stop gracefully terminates the slot's work loop and waits for it to finish.
// If the work function does not exit within stopTimeout, Stop returns anyway
// to prevent hanging the entire shutdown sequence.
// Safe to call concurrently and before Start.
func (s *Slot) Stop() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		started := s.started
		s.running = false
		s.mu.Unlock()

		close(s.stopCh)

		// If never started, close doneCh ourselves since run() will never run.
		if !started {
			s.closeDone()
		}
	})

	timer := time.NewTimer(stopTimeout)
	defer timer.Stop()

	select {
	case <-s.doneCh:
	case <-timer.C:
		s.log.Error("slot stop timed out, abandoning goroutine", "slotID", s.ID)
	}
}

// IsRunning returns whether the slot is active.
func (s *Slot) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.running
}

// SetStatus updates the slot status and last active timestamp.
func (s *Slot) SetStatus(status model.SlotStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.status = status
	s.lastActive = time.Now().Unix()
}

// Snapshot returns a thread-safe copy of slot state for heartbeat reporting.
func (s *Slot) Snapshot() model.SlotHeartbeat {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().Unix()

	return model.SlotHeartbeat{
		SlotID:     s.ID,
		Running:    s.running,
		Status:     string(s.status),
		LastActive: s.lastActive,
		Timestamp:  now,
	}
}

// closeDone safely closes doneCh exactly once, preventing double-close panics
// when Start() and Stop() race.
func (s *Slot) closeDone() {
	s.doneOnce.Do(func() {
		close(s.doneCh)
	})
}

func (s *Slot) run(parent context.Context) {
	defer s.closeDone()

	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	checkInterval := extractCheckInterval(s.Config, s.log)
	lastCheck := int64(0)
	ticker := time.NewTicker(tickInterval)

	defer ticker.Stop()

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	// Cancel context when stop signal arrives so WorkFunc can exit early.
	go func() {
		<-s.stopCh
		cancel()
	}()

	s.log.Info("slot started", "slotID", s.ID, "checkInterval", checkInterval)

	for {
		select {
		case <-s.stopCh:
			s.log.Info("slot stopped", "slotID", s.ID)

			return
		case <-ticker.C:
			s.tick(ctx, &lastCheck, int64(checkInterval))
		}
	}
}

func (s *Slot) tick(ctx context.Context, lastCheck *int64, checkInterval int64) {
	now := time.Now().Unix()
	if now-*lastCheck < checkInterval {
		return
	}

	if s.work == nil {
		s.SetStatus(model.SlotStatusIdle)

		*lastCheck = time.Now().Unix()

		return
	}

	s.SetStatus(model.SlotStatusReadPending)

	info := SlotInfo{
		ID:        s.ID,
		Config:    s.Config,
		SetStatus: s.SetStatus,
	}

	err := s.work(ctx, info)
	if err != nil {
		s.SetStatus(model.SlotStatusErrorPending)
		s.log.Error("slot work failed", "slotID", s.ID, "error", err)
		s.backoff(ctx)

		// Keep error status after backoff so the monitor can detect
		// perpetually failing slots via idle timeout.
		*lastCheck = time.Now().Unix()

		return
	}

	*lastCheck = time.Now().Unix()

	s.SetStatus(model.SlotStatusIdle)
}

func (s *Slot) backoff(ctx context.Context) {
	timer := time.NewTimer(errorBackoff)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func extractCheckInterval(config json.RawMessage, log *slog.Logger) int {
	var cfg struct {
		Bot struct {
			CheckInterval int `json:"checkInterval"`
			Interval      int `json:"interval"`
		} `json:"bot"`
	}

	err := json.Unmarshal(config, &cfg)
	if err != nil {
		log.Warn("failed to parse check interval from config, using default",
			"default", defaultInterval,
			"error", err,
		)

		return defaultInterval
	}

	// Prefer checkInterval; fall back to legacy interval key for backward
	// compatibility with configs created by the Python version.
	interval := cfg.Bot.CheckInterval
	if interval <= 0 {
		interval = cfg.Bot.Interval
	}

	if interval <= 0 {
		log.Info("check interval not set in config, using default", "default", defaultInterval)

		return defaultInterval
	}

	return interval
}
