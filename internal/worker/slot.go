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
type WorkFunc func(ctx context.Context, info SlotInfo) error

// Slot represents a single work unit running inside a container.
type Slot struct {
	ID         int
	Config     json.RawMessage
	Running    bool
	Status     model.SlotStatus
	LastActive int64

	work     WorkFunc
	mu       sync.Mutex
	stopCh   chan struct{}
	stopOnce sync.Once
	log      *slog.Logger
}

// NewSlot creates a new slot with the given ID, config, and work function.
// If work is nil, the slot idles without performing any task.
func NewSlot(slotID int, config json.RawMessage, work WorkFunc, log *slog.Logger) *Slot {
	return &Slot{
		ID:         slotID,
		Config:     config,
		Status:     model.SlotStatusIdle,
		LastActive: time.Now().Unix(),
		work:       work,
		stopCh:     make(chan struct{}),
		log:        log,
	}
}

// Start begins the slot's work loop in a goroutine.
func (s *Slot) Start(ctx context.Context) {
	s.mu.Lock()
	s.Running = true
	s.mu.Unlock()

	go s.run(ctx)
}

// Stop gracefully terminates the slot's work loop. Safe to call concurrently.
func (s *Slot) Stop() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.Running = false
		s.mu.Unlock()

		close(s.stopCh)
	})
}

// IsRunning returns whether the slot is active.
func (s *Slot) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.Running
}

// SetStatus updates the slot status and last active timestamp.
func (s *Slot) SetStatus(status model.SlotStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Status = status
	s.LastActive = time.Now().Unix()
}

// Snapshot returns a thread-safe copy of slot state for heartbeat reporting.
func (s *Slot) Snapshot() model.SlotHeartbeat {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().Unix()

	return model.SlotHeartbeat{
		SlotID:     s.ID,
		Running:    s.Running,
		Status:     string(s.Status),
		LastActive: s.LastActive,
		Timestamp:  now,
	}
}

func (s *Slot) run(parent context.Context) {
	checkInterval := extractCheckInterval(s.Config)
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

func extractCheckInterval(config json.RawMessage) int {
	var cfg struct {
		Bot struct {
			CheckInterval int `json:"checkInterval"`
		} `json:"bot"`
	}

	err := json.Unmarshal(config, &cfg)
	if err != nil || cfg.Bot.CheckInterval == 0 {
		return defaultInterval
	}

	return cfg.Bot.CheckInterval
}
