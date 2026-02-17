// Package config provides environment-based configuration for all RedTailFox components.
package config

import (
	"net"
	"os"
	"strconv"
	"time"

	redis "github.com/redis/go-redis/v9"
)

// Redis holds connection parameters for Redis.
type Redis struct {
	Host     string
	Port     string
	Password string
}

// Options returns go-redis options derived from this config.
func (r Redis) Options() *redis.Options {
	return &redis.Options{
		Addr:     net.JoinHostPort(r.Host, r.Port),
		Password: r.Password,
	}
}

// Manager holds configuration for the manager component.
type Manager struct {
	Redis                Redis
	MaxSlotsPerContainer int
	WorkerImage          string
	ContainerNamePrefix  string
	CommandChannelPrefix string
	TasksQueue           string
	ReportsQueue         string
	DBWriteQueue         string
}

// Worker holds configuration for the worker component.
type Worker struct {
	Redis          Redis
	CommandChannel string
	EventChannel   string
	ContainerName  string
}

// Monitor holds configuration for the monitor component.
type Monitor struct {
	Redis             Redis
	CheckInterval     time.Duration
	MaxSilenceSeconds int64
	SlotIdleTimeout   int64
	TasksQueue        string
}

func loadRedis() Redis {
	return Redis{
		Host:     envOrDefault("REDIS_HOST", "localhost"),
		Port:     envOrDefault("REDIS_PORT", "6379"),
		Password: envOrDefault("REDIS_PASSWORD", ""),
	}
}

// LoadManagerFromEnv creates Manager config from environment variables.
func LoadManagerFromEnv() Manager {
	const defaultMaxSlots = 10

	return Manager{
		Redis:                loadRedis(),
		MaxSlotsPerContainer: envIntOrDefault("MAX_SLOTS_PER_CONTAINER", defaultMaxSlots),
		WorkerImage:          envOrDefault("WORKER_IMAGE", "fox_worker:latest"),
		ContainerNamePrefix:  envOrDefault("WORKER_CONTAINER_PREFIX", "fox_worker"),
		CommandChannelPrefix: envOrDefault("COMMAND_CHANNEL_PREFIX", "COMMAND_CHANNEL"),
		TasksQueue:           envOrDefault("WORKER_TASKS_LIST", "manager_tasks"),
		ReportsQueue:         envOrDefault("WORKER_REPORTS_CHANNEL", "worker_reports"),
		DBWriteQueue:         envOrDefault("DB_WRITE_QUEUE", "db_write_requests"),
	}
}

// LoadWorkerFromEnv creates Worker config from environment variables.
func LoadWorkerFromEnv() Worker {
	return Worker{
		Redis:          loadRedis(),
		CommandChannel: envOrDefault("COMMAND_CHANNEL", "worker_commands"),
		EventChannel:   envOrDefault("EVENT_CHANNEL", "events"),
		ContainerName:  envOrDefault("CONTAINER_NAME", "unknown_container"),
	}
}

// LoadMonitorFromEnv creates Monitor config from environment variables.
func LoadMonitorFromEnv() Monitor {
	const (
		defaultCheckInterval = 10
		defaultMaxSilence    = 500
		defaultSlotIdle      = 600
	)

	interval := envIntOrDefault("CHECK_INTERVAL", defaultCheckInterval)

	return Monitor{
		Redis:             loadRedis(),
		CheckInterval:     time.Duration(interval) * time.Second,
		MaxSilenceSeconds: int64(envIntOrDefault("MAX_SILENCE_SECONDS", defaultMaxSilence)),
		SlotIdleTimeout:   int64(envIntOrDefault("SLOT_IDLE_TIMEOUT", defaultSlotIdle)),
		TasksQueue:        envOrDefault("WORKER_TASKS_LIST", "autoreply_queue"),
	}
}

func envOrDefault(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}

	return val
}

func envIntOrDefault(key string, fallback int) int {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}

	return parsed
}
