package manager_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/container"
	"github.com/Dark-F0X/RedTailFox/internal/manager"
	"github.com/Dark-F0X/RedTailFox/internal/model"
)

const statusRestarting = "restarting"

// mockRuntime implements container.Runtime for testing.
// All methods are safe for concurrent use via mu.
type mockRuntime struct {
	mu         sync.Mutex
	containers map[string]container.Container
	runErr     error
	stopErr    error
	removeErr  error
}

func newMockRuntime() *mockRuntime {
	return &mockRuntime{containers: make(map[string]container.Container)}
}

func (m *mockRuntime) Run(_ context.Context, opts *container.RunOptions) (container.Container, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.runErr != nil {
		return container.Container{}, m.runErr
	}

	ctr := container.Container{ID: "mock-" + opts.Name, Name: opts.Name}
	m.containers[opts.Name] = ctr

	return ctr, nil
}

func (m *mockRuntime) Stop(_ context.Context, name string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopErr != nil {
		return m.stopErr
	}

	delete(m.containers, name)

	return nil
}

func (m *mockRuntime) Remove(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.removeErr != nil {
		return m.removeErr
	}

	delete(m.containers, name)

	return nil
}

func (m *mockRuntime) List(_ context.Context, namePrefix string) ([]container.Container, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]container.Container, 0, len(m.containers))

	for _, ctr := range m.containers {
		if strings.HasPrefix(ctr.Name, namePrefix) {
			result = append(result, ctr)
		}
	}

	return result, nil
}

// containerCount returns the number of containers safely.
func (m *mockRuntime) containerCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.containers)
}

// clearContainers removes all containers (simulates them vanishing from runtime).
func (m *mockRuntime) clearContainers() {
	m.mu.Lock()
	defer m.mu.Unlock()

	clear(m.containers)
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

	if env.runtime.containerCount() != 1 {
		t.Errorf("expected 1 container, got %d", env.runtime.containerCount())
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

	if env.runtime.containerCount() != 1 {
		t.Errorf("expected 1 container (reused), got %d", env.runtime.containerCount())
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

	if env.runtime.containerCount() != 0 {
		t.Errorf("expected 0 containers after cleanup, got %d", env.runtime.containerCount())
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

func TestHandleRestartContainer(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	// Start 2 slots so a container exists with 2 slots.
	for slotID := range 2 {
		task := model.Task{
			Command: model.CommandStart,
			SlotID:  slotID + 1,
			Config:  json.RawMessage(`{}`),
		}

		if err := env.mgr.HandleTask(ctx, task); err != nil {
			t.Fatalf("failed to start slot %d: %v", slotID+1, err)
		}
	}

	if env.runtime.containerCount() != 1 {
		t.Fatalf("expected 1 container, got %d", env.runtime.containerCount())
	}

	// Drain command queues.
	env.rdb.RPop(ctx, "COMMAND_CHANNEL_1")
	env.rdb.RPop(ctx, "COMMAND_CHANNEL_1")

	// Restart the container.
	restartTask := model.Task{
		Command:       model.CommandRestartContainer,
		ContainerName: testContainerName,
	}

	if err := env.mgr.HandleTask(ctx, restartTask); err != nil {
		t.Fatalf("failed to restart container: %v", err)
	}

	// Old container should be removed and a new one created.
	if env.runtime.containerCount() != 1 {
		t.Errorf("expected 1 container after restart, got %d", env.runtime.containerCount())
	}

	// Both slots should have "restarting" db_write events.
	for range 2 {
		raw, err := env.rdb.RPop(ctx, "db_write_requests").Result()
		if err != nil {
			t.Fatalf("expected db_write event: %v", err)
		}

		var event model.DBWriteEvent
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			t.Fatalf("failed to parse db event: %v", err)
		}

		if event.Status != statusRestarting {
			t.Errorf("expected status restarting, got %s", event.Status)
		}
	}
}

func TestHandleRestartContainer_EmptyName(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	task := model.Task{
		Command:       model.CommandRestartContainer,
		ContainerName: "",
	}

	err := env.mgr.HandleTask(ctx, task)
	if err == nil {
		t.Fatal("expected error for empty container name")
	}
}

func TestHandleRestartSlot(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	// Start a slot.
	task := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	if err := env.mgr.HandleTask(ctx, task); err != nil {
		t.Fatalf("failed to start slot: %v", err)
	}

	// Drain the start command from the queue.
	env.rdb.RPop(ctx, "COMMAND_CHANNEL_1")

	// Send restart_slot.
	restartTask := model.Task{
		Command: model.CommandRestartSlot,
		SlotID:  1,
	}

	if err := env.mgr.HandleTask(ctx, restartTask); err != nil {
		t.Fatalf("failed to restart slot: %v", err)
	}

	// Old container should have received a stop command.
	stopRaw, err := env.rdb.RPop(ctx, "COMMAND_CHANNEL_1").Result()
	if err != nil {
		t.Fatalf("expected stop command in old container queue: %v", err)
	}

	var stopCmd model.Task
	if err := json.Unmarshal([]byte(stopRaw), &stopCmd); err != nil {
		t.Fatalf("failed to parse stop command: %v", err)
	}

	if stopCmd.Command != model.CommandStop {
		t.Errorf("expected stop command, got %s", stopCmd.Command)
	}

	if stopCmd.SlotID != 1 {
		t.Errorf("expected slotID 1, got %d", stopCmd.SlotID)
	}

	// New container should have received a start command (may be same or new).
	// Read all remaining commands to find the start.
	var foundStart bool

	for {
		raw, err := env.rdb.RPop(ctx, "COMMAND_CHANNEL_1").Result()
		if err != nil {
			// Try next container queue.
			raw, err = env.rdb.RPop(ctx, "COMMAND_CHANNEL_2").Result()
			if err != nil {
				break
			}
		}

		var cmd model.Task
		if err := json.Unmarshal([]byte(raw), &cmd); err != nil {
			t.Fatalf("failed to parse command: %v", err)
		}

		if cmd.Command == model.CommandStart && cmd.SlotID == 1 {
			foundStart = true

			break
		}
	}

	if !foundStart {
		t.Error("expected start command for restarted slot")
	}

	// Verify db_write event.
	dbRaw, err := env.rdb.RPop(ctx, "db_write_requests").Result()
	if err != nil {
		t.Fatalf("expected db_write event: %v", err)
	}

	var event model.DBWriteEvent
	if err := json.Unmarshal([]byte(dbRaw), &event); err != nil {
		t.Fatalf("failed to parse db event: %v", err)
	}

	if event.Status != statusRestarting {
		t.Errorf("expected status restarting, got %s", event.Status)
	}
}

func TestSyncContainers_VanishedContainer(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	// Start a slot so a container exists in both runtime and Redis.
	task := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	if err := env.mgr.HandleTask(ctx, task); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate container vanishing from runtime.
	env.runtime.clearContainers()

	// Sync should detect the vanished container and clean up state.
	env.mgr.SyncContainers(ctx)

	// Verify db_write event was published with "error" status.
	dbEvent, err := env.rdb.RPop(ctx, "db_write_requests").Result()
	if err != nil {
		t.Fatalf("expected db write event for vanished container: %v", err)
	}

	var event model.DBWriteEvent

	if err := json.Unmarshal([]byte(dbEvent), &event); err != nil {
		t.Fatalf("failed to parse db event: %v", err)
	}

	if event.Status != "error" {
		t.Errorf("expected status error, got %s", event.Status)
	}

	if event.SlotID != 1 {
		t.Errorf("expected slotID 1, got %d", event.SlotID)
	}
}

func TestHandleSlotStopped_StaleReport(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	// Start slot 1 on container A (fox_worker_1).
	task := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	err := env.mgr.HandleTask(ctx, task)
	if err != nil {
		t.Fatalf("failed to start slot: %v", err)
	}

	containerA := testContainerName

	// Full container restart moves the slot to a new container (fox_worker_2).
	restartTask := model.Task{
		Command:       model.CommandRestartContainer,
		ContainerName: containerA,
	}

	err = env.mgr.HandleTask(ctx, restartTask)
	if err != nil {
		t.Fatalf("failed to restart container: %v", err)
	}

	// Verify slot is now on a different container.
	if env.runtime.containerCount() != 1 {
		t.Fatalf("expected 1 container after restart, got %d", env.runtime.containerCount())
	}

	// Send a stale "stopped" report from the old container A.
	// It should be ignored because the slot is now on container B.
	staleReport := model.WorkerReport{
		SlotID:        1,
		Status:        "stopped",
		ContainerName: containerA,
	}

	env.mgr.HandleWorkerReport(ctx, staleReport)

	// The new container should still be running — stale report was rejected.
	if env.runtime.containerCount() != 1 {
		t.Errorf("expected 1 container still running after stale report, got %d",
			env.runtime.containerCount())
	}
}

func TestHandleSlotStopped_EmptyContainerName(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	// Start a slot so something exists.
	task := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	err := env.mgr.HandleTask(ctx, task)
	if err != nil {
		t.Fatalf("failed to start slot: %v", err)
	}

	// Send report without ContainerName — should be rejected.
	report := model.WorkerReport{
		SlotID:        1,
		Status:        "stopped",
		ContainerName: "",
	}

	env.mgr.HandleWorkerReport(ctx, report)

	// Container should still be running — the report was rejected.
	if env.runtime.containerCount() != 1 {
		t.Errorf("expected 1 container still running after rejected report, got %d",
			env.runtime.containerCount())
	}
}

func TestHandleStartTask_SendCommandFailure_Rollback(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	// Start slot 1 to create a container.
	task1 := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	err := env.mgr.HandleTask(ctx, task1)
	if err != nil {
		t.Fatalf("failed to start slot 1: %v", err)
	}

	// Delete the container channel so sendCommand fails on slot 2.
	env.rdb.HDel(ctx, "manager:container_channels", testContainerName)

	// Start slot 2 — RegisterSlot succeeds but sendCommand fails.
	task2 := model.Task{
		Command: model.CommandStart,
		SlotID:  2,
		Config:  json.RawMessage(`{}`),
	}

	err = env.mgr.HandleTask(ctx, task2)
	if err == nil {
		t.Fatal("expected error when command channel is missing")
	}

	// Verify slot 2 was rolled back (not registered).
	exists, existsErr := env.rdb.HExists(ctx, "manager:slot_to_container", "2").Result()
	if existsErr != nil {
		t.Fatalf("failed to check slot existence: %v", existsErr)
	}

	if exists {
		t.Error("slot 2 should have been rolled back after sendCommand failure")
	}
}

func TestHandleRestartContainer_StopFailure(t *testing.T) {
	env := setupManager(t)
	ctx := context.Background()

	// Start a slot so a container exists.
	task := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	if err := env.mgr.HandleTask(ctx, task); err != nil {
		t.Fatalf("failed to start slot: %v", err)
	}

	// Drain command queue.
	env.rdb.RPop(ctx, "COMMAND_CHANNEL_1")

	// Make Stop and Remove fail so the container stays running.
	env.runtime.stopErr = errors.New("stop timeout")
	env.runtime.removeErr = errors.New("container is running")

	restartTask := model.Task{
		Command:       model.CommandRestartContainer,
		ContainerName: testContainerName,
	}

	err := env.mgr.HandleTask(ctx, restartTask)
	if err == nil {
		t.Fatal("expected error when container cannot be stopped")
	}

	// Container should still be running — slots must NOT be re-created.
	if env.runtime.containerCount() != 1 {
		t.Errorf("expected container to still be running, got %d containers", env.runtime.containerCount())
	}
}
