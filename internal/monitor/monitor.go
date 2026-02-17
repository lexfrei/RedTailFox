// Package monitor checks container and slot health via Redis heartbeats.
package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/model"
)

const (
	heartbeatKeyPrefix   = "hb:container:"
	activeContainersKey  = "manager:active_containers"
	failureThreshold     = 2
	maxSlotRestarts      = 3
	maxContainerRestarts = 5
	defaultCheckInterval = 10 * time.Second
)

// Redis key prefixes for persisted failure counters.
const (
	failureKeyPrefix          = "monitor:failures:"
	slotFailureKeyPrefix      = "monitor:slot_failures:"
	containerRestartKeyPrefix = "monitor:restarts:"
	counterTTL                = time.Hour
)

// Config holds monitor settings.
type Config struct {
	// MaxSilenceSeconds is the maximum age of a heartbeat before considering the container stale.
	MaxSilenceSeconds int64

	// SlotIdleTimeout is the maximum inactivity duration for a slot before restarting it.
	SlotIdleTimeout int64

	// TasksQueue is the Redis list where restart commands are published.
	TasksQueue string

	// CheckInterval is the time between health check cycles.
	CheckInterval time.Duration
}

// Monitor periodically checks heartbeats and restarts unhealthy containers or slots.
//
// Failure counters are persisted in Redis so they survive monitor restarts.
// Counter keys use a TTL to prevent unbounded growth from orphaned entries.
type Monitor struct {
	rdb *redis.Client
	cfg Config
	log *slog.Logger
}

// New creates a new Monitor.
func New(rdb *redis.Client, cfg Config, log *slog.Logger) *Monitor {
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = defaultCheckInterval
	}

	return &Monitor{
		rdb: rdb,
		cfg: cfg,
		log: log,
	}
}

// Run starts the monitor loop with graceful shutdown on SIGTERM/SIGINT.
func (mon *Monitor) Run(ctx context.Context) {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mon.log.Info("monitor started", "interval", mon.cfg.CheckInterval)

	ticker := time.NewTicker(mon.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			mon.log.Warn("monitor shutting down")

			return
		case <-ticker.C:
			mon.Step(ctx)
		}
	}
}

// Step performs a single health check cycle. Exported for testing.
// Not safe for concurrent use — must be called from a single goroutine.
//
// NOTE: The active container set is read once and heartbeats are checked
// individually. Containers added between SMembers and heartbeat reads
// are missed until the next cycle — acceptable at a 10s interval.
func (mon *Monitor) Step(ctx context.Context) {
	containers := mon.activeContainerNames(ctx)
	seen := mon.checkHeartbeats(ctx, containers)
	mon.checkMissingContainers(ctx, containers, seen)
}

func (mon *Monitor) checkHeartbeats(ctx context.Context, containers []string) map[string]bool {
	seen := make(map[string]bool)
	now := time.Now().Unix()

	for _, containerName := range containers {
		key := heartbeatKeyPrefix + containerName

		hbt := mon.readHeartbeat(ctx, key)
		if hbt == nil {
			continue
		}

		seen[hbt.Container] = true

		if mon.isStaleHeartbeat(now, hbt.Timestamp) {
			mon.handleStaleContainer(ctx, hbt.Container)

			continue
		}

		// Heartbeat is fresh — reset failure and restart counters.
		mon.resetCounters(ctx,
			failureKeyPrefix+hbt.Container,
			containerRestartKeyPrefix+hbt.Container,
		)

		mon.checkSlots(ctx, hbt.Container, hbt.Slots, now)
	}

	return seen
}

func (mon *Monitor) activeContainerNames(ctx context.Context) []string {
	members, err := mon.rdb.SMembers(ctx, activeContainersKey).Result()
	if err != nil {
		mon.log.Error("failed to read active containers for heartbeat check", "error", err)

		return nil
	}

	return members
}

func (mon *Monitor) readHeartbeat(ctx context.Context, key string) *model.Heartbeat {
	val, err := mon.rdb.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			mon.log.Warn("heartbeat not found, container may be starting", "key", key)
		} else {
			mon.log.Error("failed to read heartbeat", "key", key, "error", err)
		}

		return nil
	}

	var hbt model.Heartbeat

	err = json.Unmarshal([]byte(val), &hbt)
	if err != nil {
		mon.log.Error("failed to parse heartbeat", "key", key, "error", err)

		return nil
	}

	return &hbt
}

func (mon *Monitor) isStaleHeartbeat(now, timestamp int64) bool {
	return now-timestamp > mon.cfg.MaxSilenceSeconds
}

func (mon *Monitor) handleStaleContainer(ctx context.Context, containerName string) {
	mon.maybeRestartContainer(ctx, containerName, "stale heartbeat")
}

// maybeRestartContainer applies the failure counter + restart rate limit
// and sends a restart_container task when thresholds are met.
// On Redis errors the check is skipped entirely — the monitor cannot
// reliably track failures or send commands without Redis.
func (mon *Monitor) maybeRestartContainer(ctx context.Context, containerName, reason string) {
	failKey := failureKeyPrefix + containerName

	count, err := mon.incrCounter(ctx, failKey)
	if err != nil {
		mon.log.Error("failed to track container failure",
			"container", containerName, "reason", reason, "error", err)

		return
	}

	if count < failureThreshold {
		mon.log.Warn("container health check failed",
			"container", containerName, "reason", reason,
			"failures", count, "threshold", failureThreshold)

		return
	}

	mon.restartContainer(ctx, containerName, reason, count, failKey)
}

func (mon *Monitor) restartContainer(
	ctx context.Context,
	containerName, reason string,
	failures int,
	failKey string,
) {
	restartKey := containerRestartKeyPrefix + containerName

	restarts, err := mon.getCounter(ctx, restartKey)
	if err != nil {
		mon.log.Error("failed to read restart count",
			"container", containerName, "error", err)

		return
	}

	if restarts >= maxContainerRestarts {
		mon.log.Error("container exceeded max restart attempts",
			"container", containerName, "reason", reason, "restarts", restarts)

		return
	}

	_, err = mon.incrCounter(ctx, restartKey)
	if err != nil {
		mon.log.Error("failed to increment restart counter",
			"container", containerName, "error", err)

		return
	}

	mon.log.Error("container unhealthy, restarting",
		"container", containerName, "reason", reason,
		"failures", failures, "restartAttempt", restarts+1)

	mon.sendTask(ctx, &model.Task{
		Command:       model.CommandRestartContainer,
		ContainerName: containerName,
		InitiatedBy:   "monitor",
	})

	mon.resetCounters(ctx, failKey)
}

func (mon *Monitor) checkSlots(ctx context.Context, containerName string, slots []model.SlotHeartbeat, now int64) {
	for idx := range slots {
		slot := &slots[idx]
		key := slotFailureKey(containerName, slot.SlotID)

		if !slot.Running {
			mon.handleUnhealthySlot(ctx, key, containerName, slot.SlotID, "slot not running")

			continue
		}

		if now-slot.LastActive > mon.cfg.SlotIdleTimeout {
			mon.handleUnhealthySlot(ctx, key, containerName, slot.SlotID,
				fmt.Sprintf("slot idle %ds", now-slot.LastActive))

			continue
		}

		// Slot is healthy — reset its failure counter.
		mon.resetCounters(ctx, slotFailureKeyPrefix+key)
	}
}

func (mon *Monitor) handleUnhealthySlot(
	ctx context.Context,
	key, containerName string,
	slotID int,
	reason string,
) {
	redisKey := slotFailureKeyPrefix + key

	count, err := mon.incrCounter(ctx, redisKey)
	if err != nil {
		mon.log.Error("failed to track slot failure",
			"container", containerName, "slotID", slotID, "error", err)

		return
	}

	if count < failureThreshold {
		mon.log.Warn("slot health check failed",
			"container", containerName,
			"slotID", slotID,
			"reason", reason,
			"failures", count,
			"threshold", failureThreshold,
		)

		return
	}

	restartsSent := count - failureThreshold
	if restartsSent >= maxSlotRestarts {
		mon.log.Error("slot restart threshold exceeded, skipping further restarts",
			"container", containerName,
			"slotID", slotID,
			"failures", count,
			"reason", reason,
		)

		return
	}

	mon.log.Warn("unhealthy slot, sending restart",
		"container", containerName,
		"slotID", slotID,
		"failures", count,
		"reason", reason,
	)

	mon.sendSlotRestart(ctx, containerName, slotID)
}

func slotFailureKey(containerName string, slotID int) string {
	return fmt.Sprintf("%s:%d", containerName, slotID)
}

func (mon *Monitor) checkMissingContainers(ctx context.Context, containers []string, seen map[string]bool) {
	for _, name := range containers {
		if seen[name] {
			continue
		}

		mon.maybeRestartContainer(ctx, name, "missing heartbeat")
	}
}

func (mon *Monitor) sendSlotRestart(ctx context.Context, containerName string, slotID int) {
	mon.sendTask(ctx, &model.Task{
		Command:       model.CommandRestartSlot,
		SlotID:        slotID,
		ContainerName: containerName,
		InitiatedBy:   "monitor",
	})
}

func (mon *Monitor) sendTask(ctx context.Context, task *model.Task) {
	data, err := json.Marshal(task)
	if err != nil {
		mon.log.Error("failed to marshal task", "error", err)

		return
	}

	err = mon.rdb.LPush(ctx, mon.cfg.TasksQueue, data).Err()
	if err != nil {
		mon.log.Error("failed to send task", "error", err)
	}
}

// incrCounter atomically increments a Redis counter and refreshes its TTL
// using a pipeline so both operations are sent in a single round-trip.
func (mon *Monitor) incrCounter(ctx context.Context, key string) (int, error) {
	pipe := mon.rdb.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, counterTTL)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "incrementing counter")
	}

	return int(incr.Val()), nil
}

// getCounter reads a Redis counter value. Returns 0 for missing keys.
func (mon *Monitor) getCounter(ctx context.Context, key string) (int, error) {
	val, err := mon.rdb.Get(ctx, key).Int()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}

		return 0, errors.Wrap(err, "reading counter")
	}

	return val, nil
}

// resetCounters deletes the given Redis counter keys.
func (mon *Monitor) resetCounters(ctx context.Context, keys ...string) {
	if len(keys) == 0 {
		return
	}

	err := mon.rdb.Del(ctx, keys...).Err()
	if err != nil {
		mon.log.Error("failed to reset counters", "keys", keys, "error", err)
	}
}
