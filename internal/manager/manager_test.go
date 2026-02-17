package manager_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/container"
	"github.com/Dark-F0X/RedTailFox/internal/manager"
	"github.com/Dark-F0X/RedTailFox/internal/model"
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

type testEnv struct {
	srv     *miniredis.Miniredis
	rdb     *redis.Client
	mgr     *manager.Manager
	runtime *mockRuntime
}

func setupManager(t *testing.T) testEnv {
	t.Helper()

	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	rt := newMockRuntime()

	cfg := manager.Config{
		MaxSlotsPerContainer: 2,
		ContainerNamePrefix:  "fox_worker",
		WorkerImage:          "fox_worker:latest",
		CommandChannelPrefix: "COMMAND_CHANNEL",
		TasksQueue:           "manager_tasks",
		ReportsQueue:         "worker_reports",
		DBWriteQueue:         "db_write_requests",
	}

	return testEnv{
		srv:     srv,
		rdb:     rdb,
		mgr:     manager.New(rdb, rt, cfg),
		runtime: rt,
	}
}

func TestHandleStartTask_NewContainer(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	task := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{"bot":{"interval":300}}`),
	}

	if err := env.mgr.HandleTask(ctx, task); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(env.runtime.containers) != 1 {
		t.Errorf("expected 1 container, got %d", len(env.runtime.containers))
	}

	cmd, err := env.rdb.RPop(ctx, "COMMAND_CHANNEL_1").Result()
	if err != nil {
		t.Fatalf("expected command in queue: %v", err)
	}

	var parsed model.Task
	if err := json.Unmarshal([]byte(cmd), &parsed); err != nil {
		t.Fatalf("failed to parse command: %v", err)
	}

	if parsed.SlotID != 1 {
		t.Errorf("expected slot_id 1, got %d", parsed.SlotID)
	}
}

func TestHandleStartTask_ExistingContainer(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	task1 := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	if err := env.mgr.HandleTask(ctx, task1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	task2 := model.Task{
		Command: model.CommandStart,
		SlotID:  2,
		Config:  json.RawMessage(`{}`),
	}

	if err := env.mgr.HandleTask(ctx, task2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(env.runtime.containers) != 1 {
		t.Errorf("expected 1 container (reused), got %d", len(env.runtime.containers))
	}
}

func TestHandleStartTask_AlreadyRunning(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	task := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	if err := env.mgr.HandleTask(ctx, task); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := env.mgr.HandleTask(ctx, task); err != nil {
		t.Fatalf("unexpected error on duplicate start: %v", err)
	}
}

func TestHandleStopTask(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	startTask := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	if err := env.mgr.HandleTask(ctx, startTask); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	env.rdb.RPop(ctx, "COMMAND_CHANNEL_1")

	stopTask := model.Task{
		Command: model.CommandStop,
		SlotID:  1,
	}

	if err := env.mgr.HandleTask(ctx, stopTask); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cmd, err := env.rdb.RPop(ctx, "COMMAND_CHANNEL_1").Result()
	if err != nil {
		t.Fatalf("expected stop command in queue: %v", err)
	}

	var parsed model.Task
	if err := json.Unmarshal([]byte(cmd), &parsed); err != nil {
		t.Fatalf("failed to parse command: %v", err)
	}

	if string(parsed.Command) != "stop" {
		t.Errorf("expected stop command, got %s", parsed.Command)
	}
}

func TestHandleWorkerReport_Stopped(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	startTask := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	if err := env.mgr.HandleTask(ctx, startTask); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	report := model.WorkerReport{
		SlotID:        1,
		Status:        "stopped",
		ContainerName: testContainerName,
	}

	env.mgr.HandleWorkerReport(ctx, report)

	if len(env.runtime.containers) != 0 {
		t.Errorf("expected 0 containers after cleanup, got %d", len(env.runtime.containers))
	}

	dbEvent, err := env.rdb.RPop(ctx, "db_write_requests").Result()
	if err != nil {
		t.Fatalf("expected db write event: %v", err)
	}

	var event model.DBWriteEvent
	if err := json.Unmarshal([]byte(dbEvent), &event); err != nil {
		t.Fatalf("failed to parse db event: %v", err)
	}

	if event.Status != "stopped" {
		t.Errorf("expected status stopped, got %s", event.Status)
	}
}

func TestHandleWorkerReport_Started(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	report := model.WorkerReport{
		SlotID: 1,
		Status: "started",
	}

	env.mgr.HandleWorkerReport(ctx, report)

	dbEvent, err := env.rdb.RPop(ctx, "db_write_requests").Result()
	if err != nil {
		t.Fatalf("expected db write event: %v", err)
	}

	var event model.DBWriteEvent
	if err := json.Unmarshal([]byte(dbEvent), &event); err != nil {
		t.Fatalf("failed to parse db event: %v", err)
	}

	if event.Status != "run_worker" {
		t.Errorf("expected status run_worker, got %s", event.Status)
	}
}
