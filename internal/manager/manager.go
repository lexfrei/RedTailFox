package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"

	"github.com/lexfrei/RedTailFox/internal/container"
	"github.com/lexfrei/RedTailFox/internal/types"
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
}

// Manager orchestrates container lifecycle and slot distribution.
type Manager struct {
	rdb     *redis.Client
	runtime container.Runtime
	state   *State
	cfg     Config
	log     *slog.Logger
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

// HandleTask dispatches a task to the appropriate handler.
func (m *Manager) HandleTask(ctx context.Context, task types.Task) error {
	switch task.Command {
	case types.CommandStart, types.CommandRun, types.CommandStartWorker:
		return m.handleStart(ctx, task)
	case types.CommandStop:
		return m.handleStop(ctx, task)
	case types.CommandRestartSlot:
		return m.handleRestartSlot(ctx, task)
	case types.CommandRestartContainer:
		return m.handleRestartContainer(ctx, task)
	default:
		return errors.Newf("unknown command: %s", task.Command)
	}
}

func (m *Manager) handleStart(ctx context.Context, task types.Task) error {
	config := task.Config

	config, err := m.resolveConfig(ctx, task.SlotID, config)
	if err != nil {
		return err
	}

	if m.state.SlotExists(ctx, task.SlotID) {
		m.log.Warn("slot already running, skipping", "slotID", task.SlotID)

		return nil
	}

	containerName, err := m.ensureContainer(ctx, task.SlotID)
	if err != nil {
		return err
	}

	m.state.RegisterSlot(ctx, task.SlotID, containerName)

	return m.sendCommand(ctx, containerName, types.Task{
		Command: types.CommandStart,
		SlotID:  task.SlotID,
		Config:  config,
	})
}

func (m *Manager) resolveConfig(ctx context.Context, slotID int, config json.RawMessage) (json.RawMessage, error) {
	if len(config) > 0 {
		if err := m.state.SaveSlotConfig(ctx, slotID, config); err != nil {
			m.log.Error("failed to save slot config", "slotID", slotID, "error", err)
		}

		return config, nil
	}

	stored, err := m.state.GetSlotConfig(ctx, slotID)
	if err != nil {
		return nil, errors.Wrapf(err, "restart impossible for slot %d", slotID)
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
	}

	if _, err := m.runtime.Run(ctx, opts); err != nil {
		m.publishDBWrite(ctx, slotID, "error", "manager", fmt.Sprintf("container_start_fail: %v", err))

		return "", errors.Wrap(err, "starting container")
	}

	return containerName, nil
}

func (m *Manager) buildContainerEnv(idx int64, name, channel string) map[string]string {
	return map[string]string{
		"REDIS_HOST":      m.cfg.RedisHost,
		"REDIS_PORT":      m.cfg.RedisPort,
		"REDIS_PASSWORD":  m.cfg.RedisPassword,
		"COMMAND_CHANNEL": channel,
		"EVENT_CHANNEL":   m.cfg.EventChannel,
		"CONTAINER_INDEX": strconv.FormatInt(idx, 10),
		"CONTAINER_NAME":  name,
	}
}

func (m *Manager) handleStop(ctx context.Context, task types.Task) error {
	containerName, err := m.state.GetSlotContainer(ctx, task.SlotID)
	if err != nil {
		m.publishDBWrite(ctx, task.SlotID, "stop", "manager", "")

		return nil
	}

	return m.sendCommand(ctx, containerName, types.Task{
		Command: types.CommandStop,
		SlotID:  task.SlotID,
	})
}

func (m *Manager) handleRestartSlot(ctx context.Context, task types.Task) error {
	m.log.Warn("restarting slot by monitor signal", "slotID", task.SlotID)

	if _, err := m.state.UnregisterSlot(ctx, task.SlotID); err != nil {
		m.log.Warn("slot not found during restart", "slotID", task.SlotID)
	}

	m.publishDBWrite(ctx, task.SlotID, "restarting", "monitor", "heartbeat_timeout")

	return m.handleStart(ctx, types.Task{
		Command: types.CommandStart,
		SlotID:  task.SlotID,
	})
}

func (m *Manager) handleRestartContainer(ctx context.Context, task types.Task) error {
	containerName := task.ContainerName
	m.log.Error("full container restart", "container", containerName)

	slots, _ := m.state.ContainerSlots(ctx, containerName)

	stopTimeout := 10 * time.Second
	if err := m.runtime.Stop(ctx, containerName, stopTimeout); err != nil {
		m.log.Error("failed to stop container", "container", containerName, "error", err)
	}

	if err := m.runtime.Remove(ctx, containerName); err != nil {
		m.log.Error("failed to remove container", "container", containerName, "error", err)
	}

	for _, sid := range slots {
		slotID, _ := strconv.Atoi(sid)
		if _, err := m.state.UnregisterSlot(ctx, slotID); err != nil {
			m.log.Warn("slot not found during container restart", "slotID", slotID)
		}

		m.publishDBWrite(ctx, slotID, "restarting", "monitor", "container_freeze")

		if err := m.handleStart(ctx, types.Task{
			Command: types.CommandStart,
			SlotID:  slotID,
		}); err != nil {
			m.log.Error("failed to restart slot", "slotID", slotID, "error", err)
		}
	}

	m.state.RemoveContainer(ctx, containerName)

	return nil
}

// HandleWorkerReport processes status reports from workers.
func (m *Manager) HandleWorkerReport(ctx context.Context, report types.WorkerReport) {
	switch {
	case report.Status == "stopped" || report.Status == "error":
		m.handleSlotStopped(ctx, report)
	case report.Status == "started":
		m.publishDBWrite(ctx, report.SlotID, "run_worker", "manager", "")
	default:
		m.log.Warn("unknown report status", "status", report.Status)
	}
}

func (m *Manager) handleSlotStopped(ctx context.Context, report types.WorkerReport) {
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

	m.state.RemoveContainer(ctx, containerName)
}

// SyncContainers detects containers that vanished from the runtime but still exist in Redis.
func (m *Manager) SyncContainers(ctx context.Context) {
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
	slots, _ := m.state.ContainerSlots(ctx, containerName)

	for _, sid := range slots {
		slotID, _ := strconv.Atoi(sid)
		if _, err := m.state.UnregisterSlot(ctx, slotID); err != nil {
			continue
		}

		m.publishDBWrite(ctx, slotID, "error", "system", "container_vanished")
	}

	m.state.RemoveContainer(ctx, containerName)
}

func (m *Manager) sendCommand(ctx context.Context, containerName string, task types.Task) error {
	queue := m.queueForContainer(containerName)

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

func (m *Manager) queueForContainer(containerName string) string {
	parts := splitLast(containerName, "_")

	return fmt.Sprintf("%s_%s", m.cfg.CommandChannelPrefix, parts)
}

func (m *Manager) publishDBWrite(ctx context.Context, slotID int, status, initiatedBy, errorText string) {
	event := types.DBWriteEvent{
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

	m.rdb.LPush(ctx, m.cfg.DBWriteQueue, data)
}

func splitLast(str, sep string) string {
	for idx := len(str) - 1; idx >= 0; idx-- {
		if string(str[idx]) == sep {
			return str[idx+1:]
		}
	}

	return str
}
