package manager_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/lexfrei/RedTailFox/internal/container"
	"github.com/lexfrei/RedTailFox/internal/manager"
	"github.com/lexfrei/RedTailFox/internal/types"
)

// mockRuntime implements container.Runtime for testing.
type mockRuntime struct {
	containers map[string]container.Container
	runErr     error
}

func newMockRuntime() *mockRuntime {
	return &mockRuntime{containers: make(map[string]container.Container)}
}

func (m *mockRuntime) Run(_ context.Context, opts container.RunOptions) (container.Container, error) {
	if m.runErr != nil {
		return container.Container{}, m.runErr
	}

	ctr := container.Container{ID: "mock-" + opts.Name, Name: opts.Name}
	m.containers[opts.Name] = ctr

	return ctr, nil
}

func (m *mockRuntime) Stop(_ context.Context, name string, _ time.Duration) error {
	delete(m.containers, name)

	return nil
}

func (m *mockRuntime) Remove(_ context.Context, name string) error {
	delete(m.containers, name)

	return nil
}

func (m *mockRuntime) List(_ context.Context, _ string) ([]container.Container, error) {
	result := make([]container.Container, 0, len(m.containers))
	for _, ctr := range m.containers {
		result = append(result, ctr)
	}

	return result, nil
}

func setupManager(t *testing.T) (*miniredis.Miniredis, *redis.Client, *manager.Manager, *mockRuntime) {
	t.Helper()

	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	runtime := newMockRuntime()

	cfg := manager.Config{
		MaxSlotsPerContainer: 2,
		ContainerNamePrefix:  "fox_worker",
		WorkerImage:          "fox_worker:latest",
		CommandChannelPrefix: "COMMAND_CHANNEL",
		TasksQueue:           "manager_tasks",
		ReportsQueue:         "worker_reports",
		DBWriteQueue:         "db_write_requests",
	}

	mgr := manager.New(rdb, runtime, cfg)

	return srv, rdb, mgr, runtime
}

func TestHandleStartTask_NewContainer(t *testing.T) {
	_, rdb, mgr, runtime := setupManager(t)
	ctx := context.Background()

	task := types.Task{
		Command: types.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{"bot":{"interval":300}}`),
	}

	err := mgr.HandleTask(ctx, task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(runtime.containers) != 1 {
		t.Errorf("expected 1 container, got %d", len(runtime.containers))
	}

	// Verify command was pushed to the queue.
	cmd, err := rdb.RPop(ctx, "COMMAND_CHANNEL_1").Result()
	if err != nil {
		t.Fatalf("expected command in queue: %v", err)
	}

	var parsed types.Task
	if err := json.Unmarshal([]byte(cmd), &parsed); err != nil {
		t.Fatalf("failed to parse command: %v", err)
	}

	if parsed.SlotID != 1 {
		t.Errorf("expected slot_id 1, got %d", parsed.SlotID)
	}
}

func TestHandleStartTask_ExistingContainer(t *testing.T) {
	_, _, mgr, runtime := setupManager(t)
	ctx := context.Background()

	// Start first slot to create a container.
	task1 := types.Task{
		Command: types.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	if err := mgr.HandleTask(ctx, task1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Start second slot — should reuse existing container.
	task2 := types.Task{
		Command: types.CommandStart,
		SlotID:  2,
		Config:  json.RawMessage(`{}`),
	}

	if err := mgr.HandleTask(ctx, task2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(runtime.containers) != 1 {
		t.Errorf("expected 1 container (reused), got %d", len(runtime.containers))
	}
}

func TestHandleStartTask_AlreadyRunning(t *testing.T) {
	_, _, mgr, _ := setupManager(t)
	ctx := context.Background()

	task := types.Task{
		Command: types.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	if err := mgr.HandleTask(ctx, task); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Second start should be a no-op (no error, just skip).
	if err := mgr.HandleTask(ctx, task); err != nil {
		t.Fatalf("unexpected error on duplicate start: %v", err)
	}
}

func TestHandleStopTask(t *testing.T) {
	_, rdb, mgr, _ := setupManager(t)
	ctx := context.Background()

	// Start a slot first.
	startTask := types.Task{
		Command: types.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}
	if err := mgr.HandleTask(ctx, startTask); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Drain the start command.
	rdb.RPop(ctx, "COMMAND_CHANNEL_1")

	// Stop the slot.
	stopTask := types.Task{
		Command: types.CommandStop,
		SlotID:  1,
	}

	if err := mgr.HandleTask(ctx, stopTask); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify stop command was pushed.
	cmd, err := rdb.RPop(ctx, "COMMAND_CHANNEL_1").Result()
	if err != nil {
		t.Fatalf("expected stop command in queue: %v", err)
	}

	var parsed types.Task
	if err := json.Unmarshal([]byte(cmd), &parsed); err != nil {
		t.Fatalf("failed to parse command: %v", err)
	}

	if string(parsed.Command) != "stop" {
		t.Errorf("expected stop command, got %s", parsed.Command)
	}
}

func TestHandleWorkerReport_Stopped(t *testing.T) {
	_, rdb, mgr, runtime := setupManager(t)
	ctx := context.Background()

	// Start a slot.
	startTask := types.Task{
		Command: types.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}
	if err := mgr.HandleTask(ctx, startTask); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Report stopped — container should be cleaned up if empty.
	report := types.WorkerReport{
		SlotID:        1,
		Status:        "stopped",
		ContainerName: "fox_worker_1",
	}

	mgr.HandleWorkerReport(ctx, report)

	// Verify container was stopped (empty now).
	if len(runtime.containers) != 0 {
		t.Errorf("expected 0 containers after cleanup, got %d", len(runtime.containers))
	}

	// Verify DB write event was published.
	dbEvent, err := rdb.RPop(ctx, "db_write_requests").Result()
	if err != nil {
		t.Fatalf("expected db write event: %v", err)
	}

	var event types.DBWriteEvent
	if err := json.Unmarshal([]byte(dbEvent), &event); err != nil {
		t.Fatalf("failed to parse db event: %v", err)
	}

	if event.Status != "stopped" {
		t.Errorf("expected status stopped, got %s", event.Status)
	}
}

func TestHandleWorkerReport_Started(t *testing.T) {
	_, rdb, mgr, _ := setupManager(t)
	ctx := context.Background()

	report := types.WorkerReport{
		SlotID: 1,
		Status: "started",
	}

	mgr.HandleWorkerReport(ctx, report)

	dbEvent, err := rdb.RPop(ctx, "db_write_requests").Result()
	if err != nil {
		t.Fatalf("expected db write event: %v", err)
	}

	var event types.DBWriteEvent
	if err := json.Unmarshal([]byte(dbEvent), &event); err != nil {
		t.Fatalf("failed to parse db event: %v", err)
	}

	if event.Status != "run_worker" {
		t.Errorf("expected status run_worker, got %s", event.Status)
	}
}
