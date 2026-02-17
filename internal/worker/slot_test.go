package worker_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/Dark-F0X/RedTailFox/internal/model"
	"github.com/Dark-F0X/RedTailFox/internal/worker"
)

func testLogger() *slog.Logger {
	return slog.Default()
}

func TestSlotStart(t *testing.T) {
	slot := worker.NewSlot(1, json.RawMessage(`{}`), nil, testLogger())
	slot.Start(context.Background())

	defer slot.Stop()

	time.Sleep(50 * time.Millisecond)

	if !slot.IsRunning() {
		t.Error("expected slot to be running")
	}
}

func TestSlotStop(t *testing.T) {
	slot := worker.NewSlot(1, json.RawMessage(`{}`), nil, testLogger())
	slot.Start(context.Background())
	time.Sleep(50 * time.Millisecond)

	slot.Stop()
	time.Sleep(50 * time.Millisecond)

	if slot.IsRunning() {
		t.Error("expected slot to be stopped")
	}
}

func TestSlotSnapshot(t *testing.T) {
	slot := worker.NewSlot(1, json.RawMessage(`{}`), nil, testLogger())
	slot.Start(context.Background())

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
	slot := worker.NewSlot(1, json.RawMessage(`{}`), nil, testLogger())

	slot.SetStatus(model.SlotStatusReadPending)

	snap := slot.Snapshot()
	if snap.Status != string(model.SlotStatusReadPending) {
		t.Errorf("expected status %s, got %s", model.SlotStatusReadPending, snap.Status)
	}
}

func TestSlotStopIdempotent(t *testing.T) {
	slot := worker.NewSlot(1, json.RawMessage(`{}`), nil, testLogger())

	// Stop without starting should not panic.
	slot.Stop()
	slot.Stop()
}

func TestSlotWorkFuncCalled(t *testing.T) {
	var called atomic.Int32

	work := func(_ context.Context, info worker.SlotInfo) error {
		if info.ID != 42 {
			t.Errorf("expected slot ID 42, got %d", info.ID)
		}

		called.Add(1)

		return nil
	}

	// Use a 0-second check interval to trigger immediately.
	cfg := json.RawMessage(`{"bot":{"checkInterval":0}}`)
	slot := worker.NewSlot(42, cfg, work, testLogger())
	slot.Start(context.Background())

	defer slot.Stop()

	// Give the slot a few ticks to call the work function.
	time.Sleep(6 * time.Second)

	if called.Load() == 0 {
		t.Error("expected work function to be called at least once")
	}
}

func TestSlotWorkFuncError(t *testing.T) {
	var callCount atomic.Int32

	errTest := errors.New("test error")

	work := func(_ context.Context, _ worker.SlotInfo) error {
		callCount.Add(1)

		return errTest
	}

	cfg := json.RawMessage(`{"bot":{"checkInterval":0}}`)
	slot := worker.NewSlot(1, cfg, work, testLogger())
	slot.Start(context.Background())

	defer slot.Stop()

	// Wait for at least one tick.
	time.Sleep(6 * time.Second)

	if callCount.Load() == 0 {
		t.Error("expected work function to be called even on error")
	}

	// After error, slot should still be running (backoff, not crash).
	if !slot.IsRunning() {
		t.Error("expected slot to keep running after work error")
	}
}

func TestSlotWorkFuncReceivesConfig(t *testing.T) {
	expectedCfg := `{"bot":{"checkInterval":1,"key":"value"}}`

	var receivedCfg json.RawMessage

	work := func(_ context.Context, info worker.SlotInfo) error {
		receivedCfg = info.Config

		return nil
	}

	slot := worker.NewSlot(1, json.RawMessage(expectedCfg), work, testLogger())
	slot.Start(context.Background())

	defer slot.Stop()

	time.Sleep(6 * time.Second)

	if string(receivedCfg) != expectedCfg {
		t.Errorf("expected config %s, got %s", expectedCfg, string(receivedCfg))
	}
}

func TestSlotWorkFuncCanSetStatus(t *testing.T) {
	work := func(_ context.Context, info worker.SlotInfo) error {
		info.SetStatus(model.SlotStatusDirectCheck)

		return nil
	}

	cfg := json.RawMessage(`{"bot":{"checkInterval":0}}`)
	slot := worker.NewSlot(1, cfg, work, testLogger())
	slot.Start(context.Background())

	defer slot.Stop()

	// Wait for work function to execute and set status.
	time.Sleep(6 * time.Second)

	// After work completes, slot should be back to idle.
	snap := slot.Snapshot()
	if snap.Status != string(model.SlotStatusIdle) {
		t.Errorf("expected idle after work, got %s", snap.Status)
	}
}
