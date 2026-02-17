package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/lexfrei/RedTailFox/internal/model"
)

const (
	heartbeatKeyPrefix = "hb:container:"
	heartbeatTTL       = time.Hour
	heartbeatInterval  = 3 * time.Second
)

// Heartbeat publishes slot health data to Redis at regular intervals.
type Heartbeat struct {
	rdb           *redis.Client
	containerName string
	slots         func() []*Slot
	log           *slog.Logger
}

// NewHeartbeat creates a heartbeat publisher.
func NewHeartbeat(rdb *redis.Client, containerName string, slotsFunc func() []*Slot, log *slog.Logger) *Heartbeat {
	return &Heartbeat{
		rdb:           rdb,
		containerName: containerName,
		slots:         slotsFunc,
		log:           log,
	}
}

// Run starts the heartbeat loop. It blocks until the context is cancelled.
func (h *Heartbeat) Run(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.publish(ctx)
		}
	}
}

// Publish sends a single heartbeat to Redis. Exported for testing.
func (h *Heartbeat) Publish(ctx context.Context) {
	h.publish(ctx)
}

func (h *Heartbeat) publish(ctx context.Context) {
	slots := h.slots()
	snapshots := make([]model.SlotHeartbeat, 0, len(slots))

	for _, slot := range slots {
		snapshots = append(snapshots, slot.Snapshot())
	}

	hbt := model.Heartbeat{
		Container: h.containerName,
		Timestamp: time.Now().Unix(),
		Slots:     snapshots,
	}

	data, err := json.Marshal(hbt)
	if err != nil {
		h.log.Error("failed to marshal heartbeat", "error", err)

		return
	}

	key := heartbeatKeyPrefix + h.containerName

	err = h.rdb.Set(ctx, key, data, heartbeatTTL).Err()
	if err != nil {
		h.log.Error("failed to publish heartbeat", "error", err)
	}
}
