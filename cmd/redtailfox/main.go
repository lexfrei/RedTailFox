// Package main is the entry point for the RedTailFox orchestrator.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/config"
	"github.com/Dark-F0X/RedTailFox/internal/container"
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

	switch os.Args[1] {
	case "manager":
		runManager(ctx)
	case "worker":
		runWorker(ctx)
	case "monitor":
		runMonitor(ctx)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func runManager(ctx context.Context) {
	cfg := config.LoadManagerFromEnv()

	if cfg.MaxSlotsPerContainer <= 0 {
		slog.Error("MAX_SLOTS_PER_CONTAINER must be positive", "value", cfg.MaxSlotsPerContainer)
		os.Exit(1)
	}

	rdb := redis.NewClient(cfg.Redis.Options())

	runtime, err := container.NewOCIRuntime(slog.Default())
	if err != nil {
		slog.Error("failed to create container runtime", "error", err)
		os.Exit(1)
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
		EventChannel:         "",
		WorkerNetwork:        cfg.WorkerNetwork,
	})

	mgr.Run(ctx)
}

func runWorker(ctx context.Context) {
	cfg := config.LoadWorkerFromEnv()
	rdb := redis.NewClient(cfg.Redis.Options())

	wrk := worker.New(rdb, cfg.ContainerName, cfg.CommandChannel, cfg.ReportsQueue, slog.Default())
	wrk.Run(ctx)
}

func runMonitor(ctx context.Context) {
	cfg := config.LoadMonitorFromEnv()
	rdb := redis.NewClient(cfg.Redis.Options())

	mon := monitor.New(rdb, monitor.Config{
		MaxSilenceSeconds: cfg.MaxSilenceSeconds,
		SlotIdleTimeout:   cfg.SlotIdleTimeout,
		TasksQueue:        cfg.TasksQueue,
		CheckInterval:     cfg.CheckInterval,
	}, slog.Default())

	mon.Run(ctx)
}
