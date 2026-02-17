// Package monitor checks container and slot health via Redis heartbeats.
package monitor

import (
	"context"
	"encoding/json"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/model"
)

const (
	heartbeatKeyPrefix   = "hb:container:"
	activeContainersKey  = "manager:active_containers"
	failureThreshold     = 2
	heartbeatKeyPattern  = heartbeatKeyPrefix + "*"
	defaultCheckInterval = 10 * time.Second
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
type Monitor struct {
	rdb      *redis.Client
	cfg      Config
	log      *slog.Logger
	failures map[string]int
}

// New creates a new Monitor.
func New(rdb *redis.Client, cfg Config, log *slog.Logger) *Monitor {
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = defaultCheckInterval
	}

	return &Monitor{
		rdb:      rdb,
		cfg:      cfg,
		log:      log,
		failures: make(map[string]int),
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
func (mon *Monitor) Step(ctx context.Context) {
	seenContainers := mon.checkHeartbeats(ctx)
	mon.checkMissingContainers(ctx, seenContainers)
}

func (mon *Monitor) checkHeartbeats(ctx context.Context) map[string]bool {
	seen := make(map[string]bool)
	now := time.Now().Unix()

	keys := mon.scanHeartbeatKeys(ctx)

	for _, key := range keys {
		hbt := mon.readHeartbeat(ctx, key)
		if hbt == nil {
			continue
		}

		seen[hbt.Container] = true

		if mon.isStaleHeartbeat(now, hbt.Timestamp) {
			mon.handleStaleContainer(ctx, hbt.Container)

			continue
		}

		// Heartbeat is fresh — reset failure counter.
		delete(mon.failures, hbt.Container)

		mon.checkSlots(ctx, hbt.Container, hbt.Slots, now)
	}

	return seen
}

func (mon *Monitor) scanHeartbeatKeys(ctx context.Context) []string {
	var keys []string

	iter := mon.rdb.Scan(ctx, 0, heartbeatKeyPattern, 0).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	err := iter.Err()
	if err != nil {
		mon.log.Error("failed to scan heartbeat keys", "error", err)
	}

	return keys
}

func (mon *Monitor) readHeartbeat(ctx context.Context, key string) *model.Heartbeat {
	val, err := mon.rdb.Get(ctx, key).Result()
	if err != nil {
		mon.log.Error("failed to read heartbeat", "key", key, "error", err)

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
	mon.failures[containerName]++
	count := mon.failures[containerName]

	if count < failureThreshold {
		mon.log.Warn("stale heartbeat detected",
			"container", containerName,
			"failures", count,
			"threshold", failureThreshold,
		)

		return
	}

	mon.log.Error("container heartbeat stale, restarting",
		"container", containerName,
		"failures", count,
	)

	mon.sendTask(ctx, &model.Task{
		Command:       model.CommandRestartContainer,
		ContainerName: containerName,
		InitiatedBy:   "monitor",
	})

	delete(mon.failures, containerName)
}

func (mon *Monitor) checkSlots(ctx context.Context, containerName string, slots []model.SlotHeartbeat, now int64) {
	for idx := range slots {
		slot := &slots[idx]

		if !slot.Running {
			mon.log.Warn("slot not running, restarting",
				"container", containerName,
				"slotID", slot.SlotID,
			)

			mon.sendSlotRestart(ctx, containerName, slot.SlotID)

			continue
		}

		if now-slot.LastActive > mon.cfg.SlotIdleTimeout {
			mon.log.Warn("slot idle too long, restarting",
				"container", containerName,
				"slotID", slot.SlotID,
				"idleSeconds", now-slot.LastActive,
			)

			mon.sendSlotRestart(ctx, containerName, slot.SlotID)
		}
	}
}

func (mon *Monitor) checkMissingContainers(ctx context.Context, seen map[string]bool) {
	members, err := mon.rdb.SMembers(ctx, activeContainersKey).Result()
	if err != nil {
		mon.log.Error("failed to read active containers", "error", err)

		return
	}

	for _, name := range members {
		if seen[name] {
			continue
		}

		mon.log.Error("container missing heartbeat, restarting", "container", name)

		mon.sendTask(ctx, &model.Task{
			Command:       model.CommandRestartContainer,
			ContainerName: name,
			InitiatedBy:   "monitor",
		})
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
