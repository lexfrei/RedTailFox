// Package main is the entry point for the RedTailFox orchestrator.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/config"
	"github.com/Dark-F0X/RedTailFox/internal/container"
	"github.com/Dark-F0X/RedTailFox/internal/errdefs"
	"github.com/Dark-F0X/RedTailFox/internal/manager"
	"github.com/Dark-F0X/RedTailFox/internal/monitor"
	"github.com/Dark-F0X/RedTailFox/internal/worker"
)

const minArgs = 2

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if len(os.Args) < minArgs {
		fmt.Fprintln(os.Stderr, "usage: redtailfox <manager|worker|monitor>")
		os.Exit(1)
	}

	ctx := context.Background()

	var err error

	switch os.Args[1] {
	case "manager":
		err = runManager(ctx)
	case "worker":
		err = runWorker(ctx)
	case "monitor":
		err = runMonitor(ctx)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}

	if err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func pingRedis(ctx context.Context, rdb *redis.Client) error {
	if err := rdb.Ping(ctx).Err(); err != nil {
		return errors.Wrap(err, "connecting to Redis")
	}

	return nil
}

func runManager(ctx context.Context) error {
	cfg := config.LoadManagerFromEnv()

	if cfg.MaxSlotsPerContainer <= 0 {
		return errors.Wrapf(errdefs.ErrInvalidConfig, "MAX_SLOTS_PER_CONTAINER must be positive, got %d", cfg.MaxSlotsPerContainer)
	}

	rdb := redis.NewClient(cfg.Redis.Options())
	if err := pingRedis(ctx, rdb); err != nil {
		return err
	}

	runtime, err := container.NewOCIRuntime(ctx, slog.Default())
	if err != nil {
		return errors.Wrap(err, "creating container runtime")
	}

	mgr := manager.New(rdb, runtime, manager.Config{
		MaxSlotsPerContainer: cfg.MaxSlotsPerContainer,
		ContainerNamePrefix:  cfg.ContainerNamePrefix,
		WorkerImage:          cfg.WorkerImage,
		CommandChannelPrefix: cfg.CommandChannelPrefix,
		TasksQueue:           cfg.TasksQueue,
		ReportsQueue:         cfg.ReportsQueue,
		DBWriteQueue:         cfg.DBWriteQueue,
		RedisHost:            cfg.Redis.Host,
		RedisPort:            cfg.Redis.Port,
		RedisPassword:        cfg.Redis.Password,
		WorkerNetwork:        cfg.WorkerNetwork,
	})

	mgr.Run(ctx)

	return nil
}

func runWorker(ctx context.Context) error {
	cfg := config.LoadWorkerFromEnv()
	rdb := redis.NewClient(cfg.Redis.Options())

	if err := pingRedis(ctx, rdb); err != nil {
		return err
	}

	wrk := worker.New(rdb, cfg.ContainerName, cfg.CommandChannel, cfg.ReportsQueue, nil, slog.Default())
	wrk.Run(ctx)

	return nil
}

func runMonitor(ctx context.Context) error {
	cfg := config.LoadMonitorFromEnv()
	rdb := redis.NewClient(cfg.Redis.Options())

	if err := pingRedis(ctx, rdb); err != nil {
		return err
	}

	mon := monitor.New(rdb, monitor.Config{
		MaxSilenceSeconds: cfg.MaxSilenceSeconds,
		SlotIdleTimeout:   cfg.SlotIdleTimeout,
		TasksQueue:        cfg.TasksQueue,
		CheckInterval:     cfg.CheckInterval,
	}, slog.Default())

	mon.Run(ctx)

	return nil
}
