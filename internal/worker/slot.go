// Package worker implements the RedTailFox container worker process.
package worker

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/Dark-F0X/RedTailFox/internal/model"
)

// Slot represents a single work unit running inside a container.
type Slot struct {
	ID         int
	Config     json.RawMessage
	Running    bool
	Status     model.SlotStatus
	LastActive int64

	mu       sync.Mutex
	stopCh   chan struct{}
	stopOnce sync.Once
	log      *slog.Logger
}

// NewSlot creates a new slot with the given ID and config.
func NewSlot(slotID int, config json.RawMessage, log *slog.Logger) *Slot {
	return &Slot{
		ID:         slotID,
		Config:     config,
		Status:     model.SlotStatusIdle,
		LastActive: time.Now().Unix(),
		stopCh:     make(chan struct{}),
		log:        log,
	}
}

// Start begins the slot's work loop in a goroutine.
func (s *Slot) Start() {
	s.mu.Lock()
	s.Running = true
	s.mu.Unlock()

	go s.run()
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

// run is a stub work loop that cycles through status transitions.
// Actual work (network calls, data processing) should be added here.
func (s *Slot) run() {
	checkInterval := extractCheckInterval(s.Config)
	lastCheck := int64(0)
	ticker := time.NewTicker(5 * time.Second)

	defer ticker.Stop()

	s.log.Info("slot started", "slotID", s.ID)

	for {
		select {
		case <-s.stopCh:
			s.log.Info("slot stopped", "slotID", s.ID)

			return
		case <-ticker.C:
			s.SetStatus(model.SlotStatusIdle)

			now := time.Now().Unix()
			if now-lastCheck >= int64(checkInterval) {
				s.SetStatus(model.SlotStatusReadPending)
				s.SetStatus(model.SlotStatusDirectCheck)

				lastCheck = time.Now().Unix()

				s.SetStatus(model.SlotStatusIdle)
			}
		}
	}
}

func extractCheckInterval(config json.RawMessage) int {
	const defaultInterval = 900

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
