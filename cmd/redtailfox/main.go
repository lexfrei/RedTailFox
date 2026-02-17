// Package main is the entry point for the RedTailFox orchestrator.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/config"
	"github.com/Dark-F0X/RedTailFox/internal/container"
	"github.com/Dark-F0X/RedTailFox/internal/errdefs"
	"github.com/Dark-F0X/RedTailFox/internal/manager"
	"github.com/Dark-F0X/RedTailFox/internal/model"
	"github.com/Dark-F0X/RedTailFox/internal/monitor"
	"github.com/Dark-F0X/RedTailFox/internal/worker"
)

const minArgs = 2

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}

	slog.Info("shutdown complete")
}

func run() error {
	if len(os.Args) < minArgs {
		return errors.Wrap(errdefs.ErrInvalidConfig, "usage: redtailfox <manager|worker|monitor>")
	}

	// Signal context covers the entire lifecycle including startup
	// so that SIGTERM/SIGINT can interrupt slow Redis or runtime init.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch os.Args[1] {
	case "manager":
		return runManager(ctx)
	case "worker":
		return runWorker(ctx)
	case "monitor":
		return runMonitor(ctx)
	default:
		return errors.Wrapf(errdefs.ErrInvalidConfig, "unknown command: %s", os.Args[1])
	}
}

func pingRedis(ctx context.Context, rdb *redis.Client) error {
	if err := rdb.Ping(ctx).Err(); err != nil {
		return errors.Wrap(err, "connecting to Redis")
	}

	return nil
}

func runManager(ctx context.Context) error {
	cfg, err := config.LoadManagerFromEnv()
	if err != nil {
		return errors.Wrap(err, "loading manager config")
	}

	if err := cfg.Redis.Validate(); err != nil {
		return errors.Wrap(err, "validating redis config")
	}

	if err := validateManagerConfig(&cfg); err != nil {
		return err
	}

	if cfg.Redis.Password != "" {
		slog.Warn("REDIS_PASSWORD is passed to worker containers as a plaintext environment variable; " +
			"use secrets management (e.g. mounted files via REDIS_PASSWORD_FILE) for production deployments")
	}

	rdb := redis.NewClient(cfg.Redis.Options())
	defer rdb.Close()

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
		RedisPasswordFile:    cfg.RedisPasswordFile,
		WorkerMemoryBytes:    cfg.WorkerMemoryBytes,
		WorkerPidsLimit:      cfg.WorkerPidsLimit,
		MaxContainers:        cfg.MaxContainers,
	})

	mgr.Run(ctx)

	return nil
}

func runWorker(ctx context.Context) error {
	cfg, err := config.LoadWorkerFromEnv()
	if err != nil {
		return errors.Wrap(err, "loading worker config")
	}

	if err := cfg.Redis.Validate(); err != nil {
		return errors.Wrap(err, "validating redis config")
	}

	rdb := redis.NewClient(cfg.Redis.Options())
	defer rdb.Close()

	if err := pingRedis(ctx, rdb); err != nil {
		return err
	}

	wrk := worker.New(rdb, cfg.ContainerName, cfg.CommandChannel, cfg.ReportsQueue, nil, slog.Default())
	wrk.Run(ctx)

	return nil
}

func runMonitor(ctx context.Context) error {
	cfg, err := config.LoadMonitorFromEnv()
	if err != nil {
		return errors.Wrap(err, "loading monitor config")
	}

	if err := cfg.Redis.Validate(); err != nil {
		return errors.Wrap(err, "validating redis config")
	}

	if err := validateMonitorConfig(&cfg); err != nil {
		return err
	}

	rdb := redis.NewClient(cfg.Redis.Options())
	defer rdb.Close()

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

func validateManagerConfig(cfg *config.Manager) error {
	if cfg.WorkerImage == "" {
		return errors.Wrap(errdefs.ErrInvalidConfig, "WORKER_IMAGE must not be empty")
	}

	if cfg.MaxSlotsPerContainer <= 0 {
		return errors.Wrapf(errdefs.ErrInvalidConfig,
			"MAX_SLOTS_PER_CONTAINER must be positive, got %d", cfg.MaxSlotsPerContainer)
	}

	if cfg.ContainerNamePrefix == "" {
		return errors.Wrap(errdefs.ErrInvalidConfig, "WORKER_CONTAINER_PREFIX must not be empty")
	}

	if !model.ContainerNameRe.MatchString(cfg.ContainerNamePrefix) {
		return errors.Wrapf(errdefs.ErrInvalidConfig,
			"WORKER_CONTAINER_PREFIX %q contains invalid characters (must match [a-zA-Z0-9][a-zA-Z0-9_.-]*)",
			cfg.ContainerNamePrefix)
	}

	if cfg.WorkerMemoryBytes < 0 {
		return errors.Wrapf(errdefs.ErrInvalidConfig,
			"WORKER_MEMORY_BYTES must be non-negative, got %d", cfg.WorkerMemoryBytes)
	}

	if cfg.WorkerPidsLimit < 0 {
		return errors.Wrapf(errdefs.ErrInvalidConfig,
			"WORKER_PIDS_LIMIT must be non-negative, got %d", cfg.WorkerPidsLimit)
	}

	return nil
}

func validateMonitorConfig(cfg *config.Monitor) error {
	if cfg.MaxSilenceSeconds <= 0 {
		return errors.Wrapf(errdefs.ErrInvalidConfig,
			"MAX_SILENCE_SECONDS must be positive, got %d", cfg.MaxSilenceSeconds)
	}

	if cfg.SlotIdleTimeout <= 0 {
		return errors.Wrapf(errdefs.ErrInvalidConfig,
			"SLOT_IDLE_TIMEOUT must be positive, got %d", cfg.SlotIdleTimeout)
	}

	if cfg.CheckInterval <= 0 {
		return errors.Wrapf(errdefs.ErrInvalidConfig,
			"CHECK_INTERVAL must be positive, got %v", cfg.CheckInterval)
	}

	return nil
}
