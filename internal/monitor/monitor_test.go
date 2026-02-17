package monitor_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/model"
	"github.com/Dark-F0X/RedTailFox/internal/monitor"
)

const (
	testTasksQueue    = "autoreply_queue"
	testContainerName = "fox_worker_1"
)

func testLogger() *slog.Logger {
	return slog.Default()
}

type testEnv struct {
	srv *miniredis.Miniredis
	rdb *redis.Client
	mon *monitor.Monitor
}

func setup(t *testing.T) testEnv {
	t.Helper()

	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})

	cfg := monitor.Config{
		MaxSilenceSeconds: 500,
		SlotIdleTimeout:   600,
		TasksQueue:        testTasksQueue,
	}

	mon := monitor.New(rdb, cfg, testLogger())

	return testEnv{srv: srv, rdb: rdb, mon: mon}
}

func publishHeartbeat(t *testing.T, srv *miniredis.Miniredis, containerName string, hbt model.Heartbeat) {
	t.Helper()

	data, err := json.Marshal(hbt)
	if err != nil {
		t.Fatalf("failed to marshal heartbeat: %v", err)
	}

	key := "hb:container:" + containerName

	err = srv.Set(key, string(data))
	if err != nil {
		t.Fatalf("failed to set heartbeat: %v", err)
	}

	srv.SetTTL(key, time.Hour)
}

func registerContainer(t *testing.T, srv *miniredis.Miniredis, name string) {
	t.Helper()

	_, err := srv.SAdd("manager:active_containers", name)
	if err != nil {
		t.Fatalf("failed to add active container: %v", err)
	}
}

func readTasks(t *testing.T, srv *miniredis.Miniredis) []string {
	t.Helper()

	if !srv.Exists(testTasksQueue) {
		return nil
	}

	tasks, err := srv.List(testTasksQueue)
	if err != nil {
		t.Fatalf("failed to read tasks: %v", err)
	}

	return tasks
}

func TestMonitorStep_HealthyContainer(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	now := time.Now().Unix()

	publishHeartbeat(t, env.srv, testContainerName, model.Heartbeat{
		Container: testContainerName,
		Timestamp: now,
		Slots: []model.SlotHeartbeat{
			{SlotID: 1, Running: true, Status: "idle", LastActive: now},
		},
	})

	registerContainer(t, env.srv, testContainerName)

	env.mon.Step(ctx)

	tasks := readTasks(t, env.srv)
	if len(tasks) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(tasks))
	}
}

func TestMonitorStep_StaleHeartbeat(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	staleTimestamp := time.Now().Unix() - 600

	publishHeartbeat(t, env.srv, testContainerName, model.Heartbeat{
		Container: testContainerName,
		Timestamp: staleTimestamp,
		Slots: []model.SlotHeartbeat{
			{SlotID: 1, Running: true, Status: "idle", LastActive: staleTimestamp},
		},
	})

	registerContainer(t, env.srv, testContainerName)

	// First check: increment failure counter but do not restart.
	env.mon.Step(ctx)

	tasks := readTasks(t, env.srv)
	if len(tasks) != 0 {
		t.Errorf("first check should not restart, got %d tasks", len(tasks))
	}

	// Second check: failure counter reaches 2, restart container.
	env.mon.Step(ctx)

	tasks = readTasks(t, env.srv)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 restart task, got %d", len(tasks))
	}

	var task model.Task

	err := json.Unmarshal([]byte(tasks[0]), &task)
	if err != nil {
		t.Fatalf("failed to parse task: %v", err)
	}

	if task.Command != model.CommandRestartContainer {
		t.Errorf("expected restart_container command, got %s", task.Command)
	}

	if task.ContainerName != testContainerName {
		t.Errorf("expected container %s, got %s", testContainerName, task.ContainerName)
	}

	if task.InitiatedBy != "monitor" {
		t.Errorf("expected initiatedBy monitor, got %s", task.InitiatedBy)
	}
}

func TestMonitorStep_IdleSlot(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	now := time.Now().Unix()
	idleTimestamp := now - 700

	publishHeartbeat(t, env.srv, testContainerName, model.Heartbeat{
		Container: testContainerName,
		Timestamp: now,
		Slots: []model.SlotHeartbeat{
			{SlotID: 1, Running: true, Status: "idle", LastActive: idleTimestamp},
		},
	})

	registerContainer(t, env.srv, testContainerName)

	env.mon.Step(ctx)

	tasks := readTasks(t, env.srv)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 restart task for idle slot, got %d", len(tasks))
	}

	var task model.Task

	err := json.Unmarshal([]byte(tasks[0]), &task)
	if err != nil {
		t.Fatalf("failed to parse task: %v", err)
	}

	if task.Command != model.CommandRestartSlot {
		t.Errorf("expected restart_slot command, got %s", task.Command)
	}

	if task.SlotID != 1 {
		t.Errorf("expected slot ID 1, got %d", task.SlotID)
	}

	if task.ContainerName != testContainerName {
		t.Errorf("expected container %s, got %s", testContainerName, task.ContainerName)
	}
}

func TestMonitorStep_SlotNotRunning(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	now := time.Now().Unix()

	publishHeartbeat(t, env.srv, testContainerName, model.Heartbeat{
		Container: testContainerName,
		Timestamp: now,
		Slots: []model.SlotHeartbeat{
			{SlotID: 1, Running: false, Status: "idle", LastActive: now},
		},
	})

	registerContainer(t, env.srv, testContainerName)

	env.mon.Step(ctx)

	tasks := readTasks(t, env.srv)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 restart task for stopped slot, got %d", len(tasks))
	}

	var task model.Task

	err := json.Unmarshal([]byte(tasks[0]), &task)
	if err != nil {
		t.Fatalf("failed to parse task: %v", err)
	}

	if task.Command != model.CommandRestartSlot {
		t.Errorf("expected restart_slot command, got %s", task.Command)
	}
}

func TestMonitorStep_MissingHeartbeat(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	// Container is registered as active but has no heartbeat key.
	registerContainer(t, env.srv, testContainerName)

	// First check: failure counter incremented but no restart yet.
	env.mon.Step(ctx)

	tasks := readTasks(t, env.srv)
	if len(tasks) != 0 {
		t.Fatalf("first check should not restart, got %d tasks", len(tasks))
	}

	// Second check: failure counter reaches threshold, restart.
	env.mon.Step(ctx)

	tasks = readTasks(t, env.srv)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 restart task for missing heartbeat, got %d", len(tasks))
	}

	var task model.Task

	err := json.Unmarshal([]byte(tasks[0]), &task)
	if err != nil {
		t.Fatalf("failed to parse task: %v", err)
	}

	if task.Command != model.CommandRestartContainer {
		t.Errorf("expected restart_container command, got %s", task.Command)
	}

	if task.ContainerName != testContainerName {
		t.Errorf("expected container %s, got %s", testContainerName, task.ContainerName)
	}
}

func TestMonitorStep_RecoveryResetsCounter(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	staleTimestamp := time.Now().Unix() - 600

	publishHeartbeat(t, env.srv, testContainerName, model.Heartbeat{
		Container: testContainerName,
		Timestamp: staleTimestamp,
		Slots: []model.SlotHeartbeat{
			{SlotID: 1, Running: true, Status: "idle", LastActive: staleTimestamp},
		},
	})

	registerContainer(t, env.srv, testContainerName)

	// First check: failure counter goes to 1.
	env.mon.Step(ctx)

	// Recovery: container sends a fresh heartbeat.
	now := time.Now().Unix()

	publishHeartbeat(t, env.srv, testContainerName, model.Heartbeat{
		Container: testContainerName,
		Timestamp: now,
		Slots: []model.SlotHeartbeat{
			{SlotID: 1, Running: true, Status: "idle", LastActive: now},
		},
	})

	// Second check: should see healthy container and reset counter.
	env.mon.Step(ctx)

	// Make it stale again.
	publishHeartbeat(t, env.srv, testContainerName, model.Heartbeat{
		Container: testContainerName,
		Timestamp: staleTimestamp,
		Slots: []model.SlotHeartbeat{
			{SlotID: 1, Running: true, Status: "idle", LastActive: staleTimestamp},
		},
	})

	// Third check: counter is 1 again (reset happened), so no restart.
	env.mon.Step(ctx)

	tasks := readTasks(t, env.srv)
	if len(tasks) != 0 {
		t.Errorf("expected 0 tasks after counter reset, got %d", len(tasks))
	}
}
