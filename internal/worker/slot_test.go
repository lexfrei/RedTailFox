package worker_test

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/Dark-F0X/RedTailFox/internal/model"
	"github.com/Dark-F0X/RedTailFox/internal/worker"
)

func testLogger() *slog.Logger {
	return slog.Default()
}

func TestSlotStart(t *testing.T) {
	slot := worker.NewSlot(1, json.RawMessage(`{}`), testLogger())
	slot.Start()

	defer slot.Stop()

	time.Sleep(50 * time.Millisecond)

	if !slot.IsRunning() {
		t.Error("expected slot to be running")
	}
}

func TestSlotStop(t *testing.T) {
	slot := worker.NewSlot(1, json.RawMessage(`{}`), testLogger())
	slot.Start()
	time.Sleep(50 * time.Millisecond)

	slot.Stop()
	time.Sleep(50 * time.Millisecond)

	if slot.IsRunning() {
		t.Error("expected slot to be stopped")
	}
}

func TestSlotSnapshot(t *testing.T) {
	slot := worker.NewSlot(1, json.RawMessage(`{}`), testLogger())
	slot.Start()

	defer slot.Stop()

	time.Sleep(50 * time.Millisecond)

	snap := slot.Snapshot()

	if snap.SlotID != 1 {
		t.Errorf("expected slot ID 1, got %d", snap.SlotID)
	}

	if !snap.Running {
		t.Error("expected running=true in snapshot")
	}
}

func TestSlotSetStatus(t *testing.T) {
	slot := worker.NewSlot(1, json.RawMessage(`{}`), testLogger())

	slot.SetStatus(model.SlotStatusReadPending)

	snap := slot.Snapshot()
	if snap.Status != string(model.SlotStatusReadPending) {
		t.Errorf("expected status %s, got %s", model.SlotStatusReadPending, snap.Status)
	}
}

func TestSlotStopIdempotent(t *testing.T) {
	slot := worker.NewSlot(1, json.RawMessage(`{}`), testLogger())

	// Stop without starting should not panic.
	slot.Stop()
	slot.Stop()
}
