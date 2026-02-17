package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/container"
	"github.com/Dark-F0X/RedTailFox/internal/errdefs"
	"github.com/Dark-F0X/RedTailFox/internal/model"
)

const (
	popTimeout               = 10 * time.Second
	syncInterval             = 30 * time.Second
	containerStopTimeout     = 10 * time.Second
	containerShutdownTimeout = 30 * time.Second
	// maxMessageSize limits the size of incoming Redis messages to prevent OOM
	// from oversized payloads pushed through the task or report queues.
	maxMessageSize = 1 << 20 // 1 MiB.
	// maxConfigSize limits the size of slot configuration JSON specifically.
	maxConfigSize = maxMessageSize
	// defaultMaxContainers is the default safety limit on the number of worker
	// containers the manager will create. Override via Config.MaxContainers.
	defaultMaxContainers = 100
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
	WorkerNetwork        string
	// RedisPasswordFile is the host path to a file containing the Redis
	// password. When set, the file is bind-mounted into worker containers
	// and REDIS_PASSWORD_FILE is used instead of REDIS_PASSWORD.
	RedisPasswordFile string
	// WorkerMemoryBytes is the memory limit for spawned worker containers.
	// Zero means no limit.
	WorkerMemoryBytes int64
	// WorkerPidsLimit is the PID limit for spawned worker containers.
	// Zero means no limit.
	WorkerPidsLimit int64
	// MaxContainers is the maximum number of worker containers the manager
	// will create. Zero uses defaultMaxContainers.
	MaxContainers int
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
	if cfg.MaxContainers <= 0 {
		cfg.MaxContainers = defaultMaxContainers
	}

	return &Manager{
		rdb:     rdb,
		runtime: runtime,
		state:   NewState(rdb, cfg.MaxSlotsPerContainer),
		cfg:     cfg,
		log:     slog.Default(),
	}
}

// drainTimeout is how long the report loop continues after the main context is
// cancelled, giving in-flight worker reports a chance to be processed.
const drainTimeout = 5 * time.Second

// Run starts the manager main loop with graceful two-phase shutdown.
// Phase 1: on signal, stop accepting new tasks and sync loop.
// Phase 2: drain remaining worker reports within drainTimeout, then exit.
// The caller is expected to pass a signal-aware context (e.g. from
// signal.NotifyContext) so that shutdown is triggered on SIGTERM/SIGINT.
//
//nolint:contextcheck // drainCtx intentionally uses background context to outlive signal cancellation.
func (m *Manager) Run(ctx context.Context) {
	m.log.Info("manager started")

	// Derive a cancellable context so that a panic in any goroutine triggers
	// shutdown of all loops instead of leaving the manager partially alive.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wgr sync.WaitGroup

	const goroutines = 3

	wgr.Add(goroutines)

	go func() {
		defer wgr.Done()
		defer m.recoverPanic("taskLoop", cancel)

		m.taskLoop(ctx)
	}()

	go func() {
		defer wgr.Done()
		defer m.recoverPanic("syncLoop", cancel)

		m.syncLoop(ctx)
	}()

	go func() {
		defer wgr.Done()
		defer m.recoverPanic("reportLoop", cancel)

		// Run report loop on main ctx, then drain remaining reports
		// with a bounded timeout so we never hang on shutdown.
		m.reportLoop(ctx)

		drainCtx, drainCancel := context.WithTimeout(context.Background(), drainTimeout)
		defer drainCancel()

		m.log.Warn("draining remaining reports", "timeout", drainTimeout)
		m.reportLoop(drainCtx)
	}()

	// wgr.Wait() guarantees all three goroutines have exited before
	// shutdownContainers runs, so no concurrent access to m.mu is possible
	// from the loop goroutines during shutdown.
	wgr.Wait()

	m.shutdownContainers()

	m.log.Warn("manager stopped")
}

// recoverPanic logs panics from manager goroutines and cancels the shared
// context so all loops shut down. Without this, a single panicking goroutine
// would leave the manager running in a degraded state.
func (m *Manager) recoverPanic(loop string, cancel context.CancelFunc) {
	if rec := recover(); rec != nil {
		m.log.Error("panic in manager goroutine, shutting down",
			"loop", loop, "panic", rec)
		cancel()
	}
}

func (m *Manager) taskLoop(ctx context.Context) {
	for {
		result, err := m.rdb.BRPop(ctx, popTimeout, m.cfg.TasksQueue).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}

			// BRPop normally returns context errors on timeout, but some
			// Redis client versions may return redis.Nil instead.
			if errors.Is(err, redis.Nil) {
				continue
			}

			m.log.Error("task listener error", "error", err)
			time.Sleep(2 * time.Second)

			continue
		}

		if len(result) < 2 {
			continue
		}

		if len(result[1]) > maxMessageSize {
			m.log.Error("task message too large, dropping",
				"size", len(result[1]), "max", maxMessageSize)

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

func (m *Manager) reportLoop(ctx context.Context) {
	for {
		result, err := m.rdb.BRPop(ctx, popTimeout, m.cfg.ReportsQueue).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}

			if errors.Is(err, redis.Nil) {
				continue
			}

			m.log.Error("report listener error", "error", err)
			time.Sleep(2 * time.Second)

			continue
		}

		if len(result) < 2 {
			continue
		}

		if len(result[1]) > maxMessageSize {
			m.log.Error("report message too large, dropping",
				"size", len(result[1]), "max", maxMessageSize)

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

func (m *Manager) syncLoop(ctx context.Context) {
	// Run an immediate sync to detect orphaned containers from a
	// previous manager instance before the first ticker fires.
	m.SyncContainers(ctx)

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

	// Commands that operate on a specific slot require a positive slot ID.
	// Zero is the default int value and could mean "not set" in JSON.
	switch task.Command {
	case model.CommandStart, model.CommandRun, model.CommandStartWorker, model.CommandStop, model.CommandRestartSlot:
		if task.SlotID <= 0 {
			return errors.Wrapf(errdefs.ErrInvalidConfig, "slot ID must be positive, got %d", task.SlotID)
		}
	case model.CommandRestartContainer:
		// Container-level commands do not need a slot ID.
	}

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

// handleStart registers and starts a slot. Caller must hold m.mu.
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
		if errors.Is(err, errdefs.ErrSlotAlreadyRunning) {
			m.log.Warn("slot registered concurrently, skipping", "slotID", task.SlotID)

			return nil
		}

		return err
	}

	sendErr := m.sendCommand(ctx, containerName, model.Task{
		Command: model.CommandStart,
		SlotID:  task.SlotID,
		Config:  config,
	})
	if sendErr != nil {
		// Roll back registration to avoid phantom slots (registered
		// in Redis but no command sent to the worker).
		if _, err := m.state.UnregisterSlot(ctx, task.SlotID); err != nil {
			m.log.Error("failed to roll back slot registration", "slotID", task.SlotID, "error", err)
		}

		return sendErr
	}

	return nil
}

func (m *Manager) resolveConfig(ctx context.Context, slotID int, config json.RawMessage) (json.RawMessage, error) {
	if len(config) > maxConfigSize {
		return nil, errors.Wrapf(errdefs.ErrInvalidConfig,
			"config for slot %d exceeds max size (%d > %d bytes)", slotID, len(config), maxConfigSize)
	}

	if len(config) > 0 && string(config) != "null" {
		if !json.Valid(config) {
			return nil, errors.Wrapf(errdefs.ErrInvalidConfig,
				"invalid JSON config for slot %d", slotID)
		}

		if err := m.state.SaveSlotConfig(ctx, slotID, config); err != nil {
			return nil, errors.Wrapf(err, "saving config for slot %d", slotID)
		}

		return config, nil
	}

	stored, err := m.state.GetSlotConfig(ctx, slotID)
	if err != nil {
		if errors.Is(err, errdefs.ErrConfigNotFound) {
			return nil, errors.Wrapf(errdefs.ErrConfigNotFound,
				"no stored config for slot %d, cannot restart without config", slotID)
		}

		return nil, errors.Wrapf(err, "loading config for slot %d", slotID)
	}

	// Re-save to refresh TTL so the config survives additional restart cycles.
	if err := m.state.SaveSlotConfig(ctx, slotID, stored); err != nil {
		m.log.Error("failed to refresh config TTL", "slotID", slotID, "error", err)
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
	active, err := m.state.ActiveContainers(ctx)
	if err != nil {
		return "", errors.Wrap(err, "counting active containers")
	}

	if len(active) >= m.cfg.MaxContainers {
		return "", errors.Wrapf(errdefs.ErrInvalidConfig,
			"container limit reached (%d/%d), cannot start new container for slot %d",
			len(active), m.cfg.MaxContainers, slotID)
	}

	idx, err := m.state.NextContainerIndex(ctx)
	if err != nil {
		return "", errors.Wrap(err, "getting container index")
	}

	containerName := fmt.Sprintf("%s_%d", m.cfg.ContainerNamePrefix, idx)
	channel := fmt.Sprintf("%s_%d", m.cfg.CommandChannelPrefix, idx)

	opts := container.RunOptions{
		Image:         m.cfg.WorkerImage,
		Name:          containerName,
		Command:       []string{"worker"},
		RestartPolicy: "unless-stopped",
		Env:           m.buildContainerEnv(idx, containerName, channel),
		Network:       m.cfg.WorkerNetwork,
		MemoryBytes:   m.cfg.WorkerMemoryBytes,
		PidsLimit:     m.cfg.WorkerPidsLimit,
	}

	if m.cfg.RedisPasswordFile != "" {
		opts.Binds = []string{m.cfg.RedisPasswordFile + ":" + workerPasswordPath + ":ro"}
	}

	if _, err := m.runtime.Run(ctx, &opts); err != nil {
		m.publishDBWrite(ctx, slotID, "error", "manager", fmt.Sprintf("container_start_fail: %v", err))

		return "", errors.Wrap(err, "starting container")
	}

	if err := m.state.SetContainerChannel(ctx, containerName, channel); err != nil {
		// Container is running but has no Redis state — clean it up to
		// prevent an orphan that sits idle until the next sync cycle.
		m.log.Error("failed to store channel, removing orphaned container",
			"container", containerName, "error", err)

		if stopErr := m.runtime.Stop(ctx, containerName, containerStopTimeout); stopErr != nil {
			m.log.Error("failed to stop orphaned container", "container", containerName, "error", stopErr)
		}

		if rmErr := m.runtime.Remove(ctx, containerName); rmErr != nil {
			m.log.Error("failed to remove orphaned container", "container", containerName, "error", rmErr)
		}

		return "", errors.Wrap(err, "storing container channel")
	}

	return containerName, nil
}

// workerPasswordPath is the in-container path where the Redis password file
// is mounted when the manager is configured with RedisPasswordFile.
const workerPasswordPath = "/run/secrets/redis_password"

func (m *Manager) buildContainerEnv(idx int64, name, channel string) map[string]string {
	env := map[string]string{
		"REDIS_HOST":             m.cfg.RedisHost,
		"REDIS_PORT":             m.cfg.RedisPort,
		"COMMAND_CHANNEL":        channel,
		"WORKER_REPORTS_CHANNEL": m.cfg.ReportsQueue,
		"CONTAINER_INDEX":        strconv.FormatInt(idx, 10),
		"CONTAINER_NAME":         name,
	}

	if m.cfg.RedisPasswordFile != "" {
		env["REDIS_PASSWORD_FILE"] = workerPasswordPath
	} else {
		env["REDIS_PASSWORD"] = m.cfg.RedisPassword
	}

	return env
}

// handleStop sends a stop command for a slot. Caller must hold m.mu.
func (m *Manager) handleStop(ctx context.Context, task model.Task) error {
	containerName, err := m.state.GetSlotContainer(ctx, task.SlotID)
	if err != nil {
		if !errors.Is(err, errdefs.ErrSlotNotFound) {
			return errors.Wrap(err, "looking up slot for stop")
		}

		m.log.Warn("slot not found for stop, ignoring", "slotID", task.SlotID)

		return nil
	}

	return m.sendCommand(ctx, containerName, model.Task{
		Command: model.CommandStop,
		SlotID:  task.SlotID,
	})
}

// handleRestartSlot unregisters and re-creates a slot. Caller must hold m.mu.
func (m *Manager) handleRestartSlot(ctx context.Context, task model.Task) error {
	m.log.Warn("restarting slot by monitor signal", "slotID", task.SlotID)

	// Unregister the slot BEFORE sending stop so that stale "stopped"
	// reports from the old worker are rejected by handleSlotStopped
	// (the slot is no longer registered, so ContainerName won't match).
	oldContainer, err := m.state.UnregisterSlot(ctx, task.SlotID)
	if err != nil {
		if !errors.Is(err, errdefs.ErrSlotNotFound) {
			// Redis error — cannot safely proceed because the old slot
			// might still be registered, risking split-brain.
			return errors.Wrap(err, "unregistering slot during restart")
		}
	}

	// Send stop to the old container after unregistration. The worker will
	// stop the slot goroutine when it dequeues this command.
	if oldContainer != "" {
		stopErr := m.sendCommand(ctx, oldContainer, model.Task{
			Command: model.CommandStop,
			SlotID:  task.SlotID,
		})
		if stopErr != nil {
			m.log.Error("failed to send stop to old container", "slotID", task.SlotID, "error", stopErr)
		}
	}

	m.publishDBWrite(ctx, task.SlotID, "restarting", "monitor", "heartbeat_timeout")

	err = m.handleStart(ctx, model.Task{
		Command: model.CommandStart,
		SlotID:  task.SlotID,
	})
	if err != nil {
		// Log and publish the error so the slot is not silently lost.
		// Common cause: stored config expired in Redis (7-day TTL).
		m.log.Error("failed to restart slot, slot requires manual re-creation",
			"slotID", task.SlotID, "error", err)
		m.publishDBWrite(ctx, task.SlotID, "error", "monitor",
			fmt.Sprintf("restart_failed: %v", err))
	}

	return err
}

// handleRestartContainer performs a kill-and-recreate restart: the old container
// is stopped and removed first, then all its slots are re-created in new
// containers. The worker inside the old container receives SIGTERM from the
// container stop. Stale "stopped" reports from the dying worker are safely
// rejected by handleSlotStopped which compares report.ContainerName against
// the current assignment before unregistering.
func (m *Manager) handleRestartContainer(ctx context.Context, task model.Task) error {
	containerName := task.ContainerName
	if containerName == "" {
		return errors.Wrap(errdefs.ErrContainerNotFound, "empty container name in restart task")
	}

	// Validate the container name belongs to this manager's namespace to
	// prevent a malicious or buggy Redis message from targeting arbitrary
	// containers (e.g., "redis", "socket-proxy").
	if !strings.HasPrefix(containerName, m.cfg.ContainerNamePrefix+"_") {
		return errors.Wrapf(errdefs.ErrInvalidConfig,
			"container name %q does not match expected prefix %q",
			containerName, m.cfg.ContainerNamePrefix)
	}

	m.log.Error("full container restart", "container", containerName)

	slots, err := m.state.ContainerSlots(ctx, containerName)
	if err != nil {
		return errors.Wrapf(err, "listing slots for container %s", containerName)
	}

	stopTimeout := containerStopTimeout
	if err := m.runtime.Stop(ctx, containerName, stopTimeout); err != nil {
		m.log.Error("failed to stop container", "container", containerName, "error", err)
	}

	if err := m.runtime.Remove(ctx, containerName); err != nil {
		m.log.Error("failed to remove container", "container", containerName, "error", err)
	}

	// Verify the old container is gone before re-creating slots to prevent
	// duplicate slot execution. If the container survived Stop+Remove, abort
	// and let the monitor retry on the next health check cycle.
	if m.isContainerRunning(ctx, containerName) {
		return errors.Wrap(errdefs.ErrContainerStillRunning, containerName)
	}

	// Remove all container state atomically (slot set, active membership,
	// command channel, and orphaned slot-to-container mappings) BEFORE
	// re-creating slots so PickContainer does not select the dead container.
	// Abort if cleanup fails — creating new slots while the dead container
	// remains in the active set would let PickContainer route them there.
	if err := m.state.RemoveContainer(ctx, containerName); err != nil {
		return errors.Wrapf(err, "removing state for dead container %s, aborting restart", containerName)
	}

	for _, sid := range slots {
		slotID, err := strconv.Atoi(sid)
		if err != nil {
			m.log.Error("invalid slot ID in container set", "sid", sid, "error", err)

			continue
		}

		m.publishDBWrite(ctx, slotID, "restarting", "monitor", "container_freeze")

		if err := m.handleStart(ctx, model.Task{
			Command: model.CommandStart,
			SlotID:  slotID,
		}); err != nil {
			m.log.Error("failed to restart slot", "slotID", slotID, "error", err)
			m.publishDBWrite(ctx, slotID, "error", "monitor", fmt.Sprintf("restart_failed: %v", err))
		}
	}

	return nil
}

// HandleWorkerReport processes status reports from workers.
// Container cleanup (Stop/Remove) runs outside the mutex so the task loop
// and sync loop remain responsive during the potentially slow runtime calls.
func (m *Manager) HandleWorkerReport(ctx context.Context, report model.WorkerReport) {
	emptyContainer := m.processReport(ctx, report)
	if emptyContainer != "" {
		m.cleanupEmptyContainer(ctx, emptyContainer)
	}
}

// processReport handles a single worker report under the mutex and returns the
// name of a container that became empty and should be cleaned up, or empty
// string if no cleanup is needed.
func (m *Manager) processReport(ctx context.Context, report model.WorkerReport) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch report.Status {
	case "stopped", "error":
		return m.handleSlotStopped(ctx, report)
	case "started":
		m.publishDBWrite(ctx, report.SlotID, "run_worker", "manager", "")
	default:
		m.log.Warn("unknown report status", "status", report.Status)
	}

	return ""
}

// handleSlotStopped processes a stopped/error report. Caller must hold m.mu.
// Returns the container name if it became empty and should be cleaned up.
func (m *Manager) handleSlotStopped(ctx context.Context, report model.WorkerReport) string {
	// Reject reports without ContainerName — all legitimate workers always
	// include it. An empty name would bypass the stale report check below.
	if report.ContainerName == "" {
		m.log.Warn("ignoring report without container name",
			"slotID", report.SlotID, "status", report.Status)

		return ""
	}

	// Guard against stale reports from a dying worker: if the slot was
	// re-registered to a different container (e.g. after container restart),
	// the report's container name won't match. Skip unregistration to
	// prevent the new assignment from being destroyed.
	// On Redis error, skip processing to avoid accidentally destroying
	// valid slot assignments.
	currentContainer, err := m.state.GetSlotContainer(ctx, report.SlotID)
	if err != nil && !errors.Is(err, errdefs.ErrSlotNotFound) {
		m.log.Error("failed to verify slot container for report, skipping",
			"slotID", report.SlotID, "error", err)

		return ""
	}

	if err == nil && currentContainer != report.ContainerName {
		m.log.Warn("ignoring stale report from old container",
			"slotID", report.SlotID,
			"reportContainer", report.ContainerName,
			"currentContainer", currentContainer,
		)

		return ""
	}

	containerName, err := m.state.UnregisterSlot(ctx, report.SlotID)
	if err != nil {
		if !errors.Is(err, errdefs.ErrSlotNotFound) {
			m.log.Error("failed to unregister slot", "slotID", report.SlotID, "error", err)

			return ""
		}

		// Slot was already unregistered (e.g., by handleRestartContainer).
		// Use report.ContainerName so we still check if the container is empty.
		m.log.Warn("slot not found during unregister", "slotID", report.SlotID)
		containerName = report.ContainerName
	}

	m.publishDBWrite(ctx, report.SlotID, report.Status, "worker", report.ErrorText)

	if containerName == "" {
		return ""
	}

	count, err := m.state.ContainerSlotCount(ctx, containerName)
	if err != nil {
		m.log.Error("failed to count container slots", "container", containerName, "error", err)

		return ""
	}

	if count > 0 {
		return ""
	}

	// Remove container from Redis active set under the lock so that
	// PickContainer will not assign new slots to it while we release
	// the mutex and perform the slow runtime.Stop/Remove calls.
	if err := m.state.RemoveContainer(ctx, containerName); err != nil {
		m.log.Error("failed to remove container state", "container", containerName, "error", err)

		return ""
	}

	return containerName
}

// cleanupEmptyContainer stops and removes a container that has no slots.
// Called without holding m.mu so the manager remains responsive.
func (m *Manager) cleanupEmptyContainer(ctx context.Context, containerName string) {
	m.log.Info("container empty, stopping", "container", containerName)

	if err := m.runtime.Stop(ctx, containerName, containerStopTimeout); err != nil {
		m.log.Error("failed to stop empty container", "container", containerName, "error", err)

		return
	}

	if err := m.runtime.Remove(ctx, containerName); err != nil {
		m.log.Error("failed to remove empty container", "container", containerName, "error", err)
	}
}

// SyncContainers detects and cleans up state mismatches:
// 1. Containers tracked in Redis but missing from the runtime (vanished).
// 2. Containers running in the runtime but not tracked in Redis (orphaned).
//
// Redis state cleanup for vanished containers runs under m.mu (fast).
// Slow runtime.Stop/Remove calls for orphaned containers run outside the
// lock so the manager remains responsive to tasks and reports.
func (m *Manager) SyncContainers(ctx context.Context) {
	orphaned := m.syncStateUnderLock(ctx)

	for _, name := range orphaned {
		m.log.Warn("orphaned container found, stopping", "container", name)
		m.stopOrphanedContainer(ctx, name)
	}
}

// syncStateUnderLock detects mismatches, cleans up vanished containers
// (Redis-only, fast), and returns orphaned container names for subsequent
// runtime cleanup outside the lock.
func (m *Manager) syncStateUnderLock(ctx context.Context) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Append "_" so that prefix "fox" does not match unrelated containers
	// like "foxtail". The manager always names workers as "<prefix>_<index>".
	actual, err := m.runtime.List(ctx, m.cfg.ContainerNamePrefix+"_")
	if err != nil {
		m.log.Error("sync: failed to list containers", "error", err)

		return nil
	}

	actualSet := make(map[string]bool, len(actual))
	for _, ctr := range actual {
		actualSet[ctr.Name] = true
	}

	redisContainers, err := m.state.ActiveContainers(ctx)
	if err != nil {
		m.log.Error("sync: failed to list redis containers", "error", err)

		return nil
	}

	redisSet := make(map[string]bool, len(redisContainers))

	for _, name := range redisContainers {
		redisSet[name] = true

		if !actualSet[name] {
			m.log.Warn("container vanished from runtime, cleaning up", "container", name)
			m.cleanupVanishedContainer(ctx, name)
		}
	}

	var result []string

	for _, ctr := range actual {
		if !redisSet[ctr.Name] {
			result = append(result, ctr.Name)
		}
	}

	return result
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
			m.log.Error("failed to unregister vanished slot", "slotID", slotID, "error", err)

			continue
		}

		m.publishDBWrite(ctx, slotID, "error", "system", "container_vanished")
	}

	if err := m.state.RemoveContainer(ctx, containerName); err != nil {
		m.log.Error("failed to remove container state", "container", containerName, "error", err)
	}
}

func (m *Manager) stopOrphanedContainer(ctx context.Context, containerName string) {
	stopTimeout := containerStopTimeout
	if err := m.runtime.Stop(ctx, containerName, stopTimeout); err != nil {
		m.log.Error("failed to stop orphaned container", "container", containerName, "error", err)

		return
	}

	if err := m.runtime.Remove(ctx, containerName); err != nil {
		m.log.Error("failed to remove orphaned container", "container", containerName, "error", err)
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

// isContainerRunning checks if a container is still present in the runtime.
func (m *Manager) isContainerRunning(ctx context.Context, name string) bool {
	// Use the container name as the prefix filter so the runtime only
	// returns matching containers instead of listing all workers.
	containers, err := m.runtime.List(ctx, name)
	if err != nil {
		m.log.Error("failed to check container status, assuming running", "error", err)

		return true
	}

	for _, ctr := range containers {
		if ctr.Name == name {
			return true
		}
	}

	return false
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

// shutdownContainers stops and removes all tracked worker containers
// concurrently during graceful shutdown. Uses a background context since
// the signal context is already cancelled at this point.
func (m *Manager) shutdownContainers() {
	m.mu.Lock()
	defer m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), containerShutdownTimeout)
	defer cancel()

	containers, err := m.state.ActiveContainers(ctx)
	if err != nil {
		m.log.Error("shutdown: failed to list active containers", "error", err)

		return
	}

	if len(containers) == 0 {
		return
	}

	m.log.Info("shutdown: stopping worker containers", "count", len(containers))

	var wgr sync.WaitGroup

	wgr.Add(len(containers))

	for _, name := range containers {
		go m.shutdownOneContainer(ctx, &wgr, name)
	}

	wgr.Wait()
}

func (m *Manager) shutdownOneContainer(ctx context.Context, wgr *sync.WaitGroup, name string) {
	defer wgr.Done()

	if err := m.runtime.Stop(ctx, name, containerStopTimeout); err != nil {
		m.log.Error("shutdown: failed to stop container", "container", name, "error", err)
	}

	if err := m.runtime.Remove(ctx, name); err != nil {
		m.log.Error("shutdown: failed to remove container", "container", name, "error", err)
	}

	if err := m.state.RemoveContainer(ctx, name); err != nil {
		m.log.Error("shutdown: failed to clean container state", "container", name, "error", err)
	}
}
