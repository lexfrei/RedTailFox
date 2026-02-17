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

const workFuncTimeout = 10 * time.Second

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
	called := make(chan struct{}, 1)

	work := func(_ context.Context, info worker.SlotInfo) error {
		if info.ID != 42 {
			t.Errorf("expected slot ID 42, got %d", info.ID)
		}

		select {
		case called <- struct{}{}:
		default:
		}

		return nil
	}

	// Use a 0-second check interval to trigger immediately.
	cfg := json.RawMessage(`{"bot":{"checkInterval":1}}`)
	slot := worker.NewSlot(42, cfg, work, testLogger())
	slot.Start(context.Background())

	defer slot.Stop()

	select {
	case <-called:
	case <-time.After(workFuncTimeout):
		t.Fatal("timed out waiting for work function to be called")
	}
}

func TestSlotWorkFuncError(t *testing.T) {
	called := make(chan struct{}, 1)
	errTest := errors.New("test error")

	work := func(_ context.Context, _ worker.SlotInfo) error {
		select {
		case called <- struct{}{}:
		default:
		}

		return errTest
	}

	cfg := json.RawMessage(`{"bot":{"checkInterval":1}}`)
	slot := worker.NewSlot(1, cfg, work, testLogger())
	slot.Start(context.Background())

	defer slot.Stop()

	select {
	case <-called:
	case <-time.After(workFuncTimeout):
		t.Fatal("timed out waiting for work function to be called")
	}

	// After error, slot should still be running (backoff, not crash).
	if !slot.IsRunning() {
		t.Error("expected slot to keep running after work error")
	}
}

func TestSlotWorkFuncReceivesConfig(t *testing.T) {
	expectedCfg := `{"bot":{"checkInterval":1,"key":"value"}}`
	received := make(chan json.RawMessage, 1)

	work := func(_ context.Context, info worker.SlotInfo) error {
		select {
		case received <- info.Config:
		default:
		}

		return nil
	}

	slot := worker.NewSlot(1, json.RawMessage(expectedCfg), work, testLogger())
	slot.Start(context.Background())

	defer slot.Stop()

	select {
	case cfg := <-received:
		if string(cfg) != expectedCfg {
			t.Errorf("expected config %s, got %s", expectedCfg, string(cfg))
		}
	case <-time.After(workFuncTimeout):
		t.Fatal("timed out waiting for work function to receive config")
	}
}

func TestSlotWorkFuncCanSetStatus(t *testing.T) {
	var callCount atomic.Int32

	done := make(chan struct{})

	work := func(_ context.Context, info worker.SlotInfo) error {
		info.SetStatus(model.SlotStatusDirectCheck)

		if callCount.Add(1) == 1 {
			close(done)
		}

		return nil
	}

	cfg := json.RawMessage(`{"bot":{"checkInterval":1}}`)
	slot := worker.NewSlot(1, cfg, work, testLogger())
	slot.Start(context.Background())

	defer slot.Stop()

	select {
	case <-done:
	case <-time.After(workFuncTimeout):
		t.Fatal("timed out waiting for work function to execute")
	}

	// Give tick() a moment to reset status to idle after work returns.
	time.Sleep(50 * time.Millisecond)

	// After work completes, slot should be back to idle.
	snap := slot.Snapshot()
	if snap.Status != string(model.SlotStatusIdle) {
		t.Errorf("expected idle after work, got %s", snap.Status)
	}
}

func TestSlotRunningFalseAfterRunExits(t *testing.T) {
	slot := worker.NewSlot(1, json.RawMessage(`{}`), nil, testLogger())
	slot.Start(context.Background())

	time.Sleep(50 * time.Millisecond)

	slot.Stop()

	if slot.IsRunning() {
		t.Error("expected running=false after run exits")
	}
}
