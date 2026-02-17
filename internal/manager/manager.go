package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/container"
	"github.com/Dark-F0X/RedTailFox/internal/errdefs"
	"github.com/Dark-F0X/RedTailFox/internal/model"
)

const (
	popTimeout   = 10 * time.Second
	syncInterval = 30 * time.Second
)

// Config holds manager-specific settings.
type Config struct {
	MaxSlotsPerContainer int
	ContainerNamePrefix  string
	WorkerImage          string
	CommandChannelPrefix string
	TasksQueue           string
	ReportsQueue         string
	DBWriteQueue         string
	RedisHost            string
	RedisPort            string
	RedisPassword        string
	EventChannel         string
	WorkerNetwork        string
}

// Manager orchestrates container lifecycle and slot distribution.
// All container lifecycle operations are serialized via mu to prevent
// TOCTOU races between taskLoop, reportLoop, and syncLoop.
type Manager struct {
	rdb     *redis.Client
	runtime container.Runtime
	state   *State
	cfg     Config
	log     *slog.Logger
	mu      sync.Mutex
}

// New creates a new Manager instance.
func New(rdb *redis.Client, runtime container.Runtime, cfg Config) *Manager {
	return &Manager{
		rdb:     rdb,
		runtime: runtime,
		state:   NewState(rdb, cfg.MaxSlotsPerContainer),
		cfg:     cfg,
		log:     slog.Default(),
	}
}

// Run starts the manager main loop with graceful shutdown.
func (m *Manager) Run(ctx context.Context) {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	m.log.Info("manager started")

	var wgr sync.WaitGroup

	const goroutines = 3 // task loop, report listener, sync loop.

	wgr.Add(goroutines)

	go func() {
		defer wgr.Done()

		m.taskLoop(ctx)
	}()

	go func() {
		defer wgr.Done()

		m.reportLoop(ctx)
	}()

	go func() {
		defer wgr.Done()

		m.syncLoop(ctx)
	}()

	wgr.Wait()

	m.log.Warn("manager stopped")
}

func (m *Manager) taskLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			result, err := m.rdb.BRPop(ctx, popTimeout, m.cfg.TasksQueue).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
					continue
				}

				m.log.Error("task listener error", "error", err)
				time.Sleep(2 * time.Second)

				continue
			}

			if len(result) < 2 {
				continue
			}

			var task model.Task

			err = json.Unmarshal([]byte(result[1]), &task)
			if err != nil {
				m.log.Error("failed to parse task", "error", err)

				continue
			}

			if err := m.HandleTask(ctx, task); err != nil {
				m.log.Error("failed to handle task", "error", err)
			}
		}
	}
}

func (m *Manager) reportLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			result, err := m.rdb.BRPop(ctx, popTimeout, m.cfg.ReportsQueue).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
					continue
				}

				m.log.Error("report listener error", "error", err)
				time.Sleep(2 * time.Second)

				continue
			}

			if len(result) < 2 {
				continue
			}

			var report model.WorkerReport

			err = json.Unmarshal([]byte(result[1]), &report)
			if err != nil {
				m.log.Error("failed to parse report", "error", err)

				continue
			}

			m.HandleWorkerReport(ctx, report)
		}
	}
}

func (m *Manager) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.SyncContainers(ctx)
		}
	}
}

// HandleTask dispatches a task to the appropriate handler.
func (m *Manager) HandleTask(ctx context.Context, task model.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch task.Command {
	case model.CommandStart, model.CommandRun, model.CommandStartWorker:
		return m.handleStart(ctx, task)
	case model.CommandStop:
		return m.handleStop(ctx, task)
	case model.CommandRestartSlot:
		return m.handleRestartSlot(ctx, task)
	case model.CommandRestartContainer:
		return m.handleRestartContainer(ctx, task)
	default:
		return errors.Wrapf(errdefs.ErrUnknownCommand, "command: %s", task.Command)
	}
}

func (m *Manager) handleStart(ctx context.Context, task model.Task) error {
	config := task.Config

	config, err := m.resolveConfig(ctx, task.SlotID, config)
	if err != nil {
		return err
	}

	exists, err := m.state.SlotExists(ctx, task.SlotID)
	if err != nil {
		// On Redis error, assume slot exists to prevent duplicates.
		m.log.Error("failed to check slot existence, assuming exists", "slotID", task.SlotID, "error", err)

		return errors.Wrap(err, "checking slot existence")
	}

	if exists {
		m.log.Warn("slot already running, skipping", "slotID", task.SlotID)

		return nil
	}

	containerName, err := m.ensureContainer(ctx, task.SlotID)
	if err != nil {
		return err
	}

	if err := m.state.RegisterSlot(ctx, task.SlotID, containerName); err != nil {
		return err
	}

	return m.sendCommand(ctx, containerName, model.Task{
		Command: model.CommandStart,
		SlotID:  task.SlotID,
		Config:  config,
	})
}

func (m *Manager) resolveConfig(ctx context.Context, slotID int, config json.RawMessage) (json.RawMessage, error) {
	if len(config) > 0 {
		if err := m.state.SaveSlotConfig(ctx, slotID, config); err != nil {
			return nil, errors.Wrapf(err, "saving config for slot %d", slotID)
		}

		return config, nil
	}

	stored, err := m.state.GetSlotConfig(ctx, slotID)
	if err != nil {
		m.log.Warn("no stored config found, using empty config",
			"slotID", slotID,
			"error", err,
		)

		return json.RawMessage("{}"), nil
	}

	return stored, nil
}

func (m *Manager) ensureContainer(ctx context.Context, slotID int) (string, error) {
	containerName, err := m.state.PickContainer(ctx)
	if err != nil {
		return "", errors.Wrap(err, "picking container")
	}

	if containerName != "" {
		return containerName, nil
	}

	return m.startNewContainer(ctx, slotID)
}

func (m *Manager) startNewContainer(ctx context.Context, slotID int) (string, error) {
	idx, err := m.state.NextContainerIndex(ctx)
	if err != nil {
		return "", errors.Wrap(err, "getting container index")
	}

	containerName := fmt.Sprintf("%s_%d", m.cfg.ContainerNamePrefix, idx)
	channel := fmt.Sprintf("%s_%d", m.cfg.CommandChannelPrefix, idx)

	opts := container.RunOptions{
		Image:         m.cfg.WorkerImage,
		Name:          containerName,
		RestartPolicy: "unless-stopped",
		Env:           m.buildContainerEnv(idx, containerName, channel),
		Network:       m.cfg.WorkerNetwork,
	}

	if _, err := m.runtime.Run(ctx, opts); err != nil {
		m.publishDBWrite(ctx, slotID, "error", "manager", fmt.Sprintf("container_start_fail: %v", err))

		return "", errors.Wrap(err, "starting container")
	}

	if err := m.state.SetContainerChannel(ctx, containerName, channel); err != nil {
		m.log.Error("failed to store container channel", "container", containerName, "error", err)
	}

	return containerName, nil
}

func (m *Manager) buildContainerEnv(idx int64, name, channel string) map[string]string {
	return map[string]string{
		"REDIS_HOST":             m.cfg.RedisHost,
		"REDIS_PORT":             m.cfg.RedisPort,
		"REDIS_PASSWORD":         m.cfg.RedisPassword,
		"COMMAND_CHANNEL":        channel,
		"EVENT_CHANNEL":          m.cfg.EventChannel,
		"WORKER_REPORTS_CHANNEL": m.cfg.ReportsQueue,
		"CONTAINER_INDEX":        strconv.FormatInt(idx, 10),
		"CONTAINER_NAME":         name,
	}
}

func (m *Manager) handleStop(ctx context.Context, task model.Task) error {
	containerName, err := m.state.GetSlotContainer(ctx, task.SlotID)
	if err != nil {
		if !errors.Is(err, errdefs.ErrSlotNotFound) {
			return errors.Wrap(err, "looking up slot for stop")
		}

		m.publishDBWrite(ctx, task.SlotID, "stop", "manager", "")
		m.log.Warn("slot not found for stop, publishing status anyway", "slotID", task.SlotID)

		return nil
	}

	return m.sendCommand(ctx, containerName, model.Task{
		Command: model.CommandStop,
		SlotID:  task.SlotID,
	})
}

func (m *Manager) handleRestartSlot(ctx context.Context, task model.Task) error {
	m.log.Warn("restarting slot by monitor signal", "slotID", task.SlotID)

	if _, err := m.state.UnregisterSlot(ctx, task.SlotID); err != nil {
		m.log.Warn("slot not found during restart", "slotID", task.SlotID)
	}

	m.publishDBWrite(ctx, task.SlotID, "restarting", "monitor", "heartbeat_timeout")

	return m.handleStart(ctx, model.Task{
		Command: model.CommandStart,
		SlotID:  task.SlotID,
	})
}

func (m *Manager) handleRestartContainer(ctx context.Context, task model.Task) error {
	containerName := task.ContainerName
	m.log.Error("full container restart", "container", containerName)

	slots, err := m.state.ContainerSlots(ctx, containerName)
	if err != nil {
		return errors.Wrapf(err, "listing slots for container %s", containerName)
	}

	stopTimeout := 10 * time.Second
	if err := m.runtime.Stop(ctx, containerName, stopTimeout); err != nil {
		m.log.Error("failed to stop container", "container", containerName, "error", err)
	}

	if err := m.runtime.Remove(ctx, containerName); err != nil {
		m.log.Error("failed to remove container", "container", containerName, "error", err)
	}

	// Remove container state BEFORE re-creating slots so PickContainer does not
	// select the dead container for new slot assignments.
	if err := m.state.RemoveContainer(ctx, containerName); err != nil {
		m.log.Error("failed to remove container state", "container", containerName, "error", err)
	}

	for _, sid := range slots {
		slotID, err := strconv.Atoi(sid)
		if err != nil {
			m.log.Error("invalid slot ID in container set", "sid", sid, "error", err)

			continue
		}

		if _, err := m.state.UnregisterSlot(ctx, slotID); err != nil {
			m.log.Warn("slot not found during container restart", "slotID", slotID)
		}

		m.publishDBWrite(ctx, slotID, "restarting", "monitor", "container_freeze")

		if err := m.handleStart(ctx, model.Task{
			Command: model.CommandStart,
			SlotID:  slotID,
		}); err != nil {
			m.log.Error("failed to restart slot", "slotID", slotID, "error", err)
		}
	}

	return nil
}

// HandleWorkerReport processes status reports from workers.
func (m *Manager) HandleWorkerReport(ctx context.Context, report model.WorkerReport) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch report.Status {
	case "stopped", "error":
		m.handleSlotStopped(ctx, report)
	case "started":
		m.publishDBWrite(ctx, report.SlotID, "run_worker", "manager", "")
	default:
		m.log.Warn("unknown report status", "status", report.Status)
	}
}

func (m *Manager) handleSlotStopped(ctx context.Context, report model.WorkerReport) {
	containerName, err := m.state.UnregisterSlot(ctx, report.SlotID)
	if err != nil {
		m.log.Warn("could not unregister slot", "slotID", report.SlotID)
	}

	m.publishDBWrite(ctx, report.SlotID, report.Status, "worker", report.ErrorText)

	if containerName == "" {
		return
	}

	count, err := m.state.ContainerSlotCount(ctx, containerName)
	if err != nil || count > 0 {
		return
	}

	m.log.Info("container empty, stopping", "container", containerName)

	stopTimeout := 10 * time.Second
	if err := m.runtime.Stop(ctx, containerName, stopTimeout); err != nil {
		m.log.Error("failed to stop empty container", "container", containerName, "error", err)
	}

	if err := m.runtime.Remove(ctx, containerName); err != nil {
		m.log.Error("failed to remove empty container", "container", containerName, "error", err)
	}

	if err := m.state.RemoveContainer(ctx, containerName); err != nil {
		m.log.Error("failed to remove container state", "container", containerName, "error", err)
	}
}

// SyncContainers detects containers that vanished from the runtime but still exist in Redis.
func (m *Manager) SyncContainers(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	actual, err := m.runtime.List(ctx, m.cfg.ContainerNamePrefix)
	if err != nil {
		m.log.Error("sync: failed to list containers", "error", err)

		return
	}

	actualSet := make(map[string]bool, len(actual))
	for _, ctr := range actual {
		actualSet[ctr.Name] = true
	}

	redisContainers, err := m.state.ActiveContainers(ctx)
	if err != nil {
		m.log.Error("sync: failed to list redis containers", "error", err)

		return
	}

	for _, name := range redisContainers {
		if actualSet[name] {
			continue
		}

		m.log.Warn("container vanished from runtime, cleaning up", "container", name)
		m.cleanupVanishedContainer(ctx, name)
	}
}

func (m *Manager) cleanupVanishedContainer(ctx context.Context, containerName string) {
	slots, err := m.state.ContainerSlots(ctx, containerName)
	if err != nil {
		m.log.Error("failed to list slots for vanished container", "container", containerName, "error", err)

		return
	}

	for _, sid := range slots {
		slotID, err := strconv.Atoi(sid)
		if err != nil {
			m.log.Error("invalid slot ID in container set", "sid", sid, "error", err)

			continue
		}

		if _, err := m.state.UnregisterSlot(ctx, slotID); err != nil {
			continue
		}

		m.publishDBWrite(ctx, slotID, "error", "system", "container_vanished")
	}

	if err := m.state.RemoveContainer(ctx, containerName); err != nil {
		m.log.Error("failed to remove container state", "container", containerName, "error", err)
	}
}

func (m *Manager) sendCommand(ctx context.Context, containerName string, task model.Task) error {
	queue, err := m.state.GetContainerChannel(ctx, containerName)
	if err != nil {
		return errors.Wrapf(err, "looking up command channel for %s", containerName)
	}

	data, err := json.Marshal(task)
	if err != nil {
		return errors.Wrap(err, "marshaling command")
	}

	if err := m.rdb.LPush(ctx, queue, data).Err(); err != nil {
		return errors.Wrap(err, "sending command to container queue")
	}

	m.log.Info("command sent", "command", task.Command, "slotID", task.SlotID, "container", containerName)

	return nil
}

func (m *Manager) publishDBWrite(ctx context.Context, slotID int, status, initiatedBy, errorText string) {
	event := model.DBWriteEvent{
		SlotID:      slotID,
		Status:      status,
		InitiatedBy: initiatedBy,
		Timestamp:   time.Now().Unix(),
		ErrorText:   errorText,
	}

	data, err := json.Marshal(event)
	if err != nil {
		m.log.Error("failed to marshal db write event", "error", err)

		return
	}

	err = m.rdb.LPush(ctx, m.cfg.DBWriteQueue, data).Err()
	if err != nil {
		m.log.Error("failed to publish db write event", "error", err)
	}
}
