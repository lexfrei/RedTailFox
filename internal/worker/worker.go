package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/model"
)

const (
	defaultReportsQueue = "worker_reports"
	commandTimeout      = 10 * time.Second
	shutdownDelay       = time.Second
)

// Worker manages slots inside a container and listens for commands.
type Worker struct {
	rdb           *redis.Client
	containerName string
	commandCh     string
	reportsQueue  string
	slots         map[string]*Slot
	mu            sync.Mutex
	log           *slog.Logger
}

// New creates a new Worker.
func New(rdb *redis.Client, containerName, commandChannel, reportsQueue string, log *slog.Logger) *Worker {
	if reportsQueue == "" {
		reportsQueue = defaultReportsQueue
	}

	return &Worker{
		rdb:           rdb,
		containerName: containerName,
		commandCh:     commandChannel,
		reportsQueue:  reportsQueue,
		slots:         make(map[string]*Slot),
		log:           log,
	}
}

// Run starts the worker's main loop with graceful shutdown on SIGTERM/SIGINT.
func (w *Worker) Run(ctx context.Context) {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	hbt := NewHeartbeat(w.rdb, w.containerName, w.slotList, w.log)

	go hbt.Run(ctx)

	w.log.Info("worker ready", "container", w.containerName, "channel", w.commandCh)
	w.commandLoop(ctx)

	w.log.Warn("shutting down, stopping all slots")
	w.stopAllSlots()
	time.Sleep(shutdownDelay)
}

// HandleCommand processes a single command message. Exported for testing.
func (w *Worker) HandleCommand(ctx context.Context, raw []byte) {
	var task model.Task

	err := json.Unmarshal(raw, &task)
	if err != nil {
		w.log.Error("failed to parse command", "error", err)

		return
	}

	slotID := slotKey(task.SlotID)

	switch task.Command {
	case model.CommandStart:
		w.handleStart(ctx, slotID, &task)
	case model.CommandStop:
		w.handleStop(ctx, slotID, task.SlotID)
	case model.CommandRestartSlot, model.CommandRestartContainer, model.CommandRun, model.CommandStartWorker:
		w.log.Warn("unsupported command in worker", "command", task.Command)
	default:
		w.log.Warn("unknown command", "command", task.Command)
	}
}

func (w *Worker) handleStart(ctx context.Context, slotID string, task *model.Task) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if existing, ok := w.slots[slotID]; ok && existing.IsRunning() {
		w.log.Warn("slot already active", "slotID", slotID)

		return
	}

	slot := NewSlot(task.SlotID, task.Config, w.log)
	w.slots[slotID] = slot
	slot.Start()

	w.sendReport(ctx, task.SlotID, "started", "")
}

func (w *Worker) handleStop(ctx context.Context, slotKey string, originalID int) {
	w.mu.Lock()
	slot, ok := w.slots[slotKey]

	if !ok {
		w.mu.Unlock()
		w.sendReport(ctx, originalID, "stopped", "")

		return
	}

	delete(w.slots, slotKey)
	w.mu.Unlock()

	slot.Stop()
	w.sendReport(ctx, slot.ID, "stopped", "")
}

func (w *Worker) sendReport(ctx context.Context, slotID int, status, errorText string) {
	report := model.WorkerReport{
		SlotID:        slotID,
		Status:        status,
		ContainerName: w.containerName,
		InitiatedBy:   "worker",
		ErrorText:     errorText,
	}

	data, err := json.Marshal(report)
	if err != nil {
		w.log.Error("failed to marshal report", "error", err)

		return
	}

	err = w.rdb.LPush(ctx, w.reportsQueue, data).Err()
	if err != nil {
		w.log.Error("failed to send report", "error", err)
	}
}

func (w *Worker) commandLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			result, err := w.rdb.BRPop(ctx, commandTimeout, w.commandCh).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
					continue
				}

				w.log.Error("command listener error", "error", err)
				time.Sleep(2 * time.Second)

				continue
			}

			if len(result) >= 2 {
				w.HandleCommand(ctx, []byte(result[1]))
			}
		}
	}
}

func (w *Worker) slotList() []*Slot {
	w.mu.Lock()
	defer w.mu.Unlock()

	result := make([]*Slot, 0, len(w.slots))
	for _, slot := range w.slots {
		result = append(result, slot)
	}

	return result
}

func (w *Worker) stopAllSlots() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, slot := range w.slots {
		slot.Stop()
	}
}

func slotKey(slotID int) string {
	return strconv.Itoa(slotID)
}
