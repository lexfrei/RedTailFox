// Package config provides environment-based configuration for all RedTailFox components.
package config

import (
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	redis "github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/errdefs"
)

// Redis holds connection parameters for Redis.
type Redis struct {
	Host     string
	Port     string
	Password string
}

const (
	defaultDialTimeout  = 5 * time.Second
	defaultReadTimeout  = 3 * time.Second
	defaultWriteTimeout = 3 * time.Second
)

// Validate checks that Redis connection parameters are well-formed.
func (r Redis) Validate() error {
	if r.Host == "" {
		return errors.Wrap(errdefs.ErrInvalidConfig, "REDIS_HOST must not be empty")
	}

	const (
		minPort = 1
		maxPort = 65535
	)

	port, err := strconv.Atoi(r.Port)
	if err != nil {
		return errors.Wrapf(errdefs.ErrInvalidConfig, "invalid redis port %q", r.Port)
	}

	if port < minPort || port > maxPort {
		return errors.Wrapf(errdefs.ErrInvalidConfig, "redis port %d out of range [%d, %d]", port, minPort, maxPort)
	}

	return nil
}

// Options returns go-redis options derived from this config.
func (r Redis) Options() *redis.Options {
	return &redis.Options{
		Addr:         net.JoinHostPort(r.Host, r.Port),
		Password:     r.Password,
		DialTimeout:  defaultDialTimeout,
		ReadTimeout:  defaultReadTimeout,
		WriteTimeout: defaultWriteTimeout,
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
	WorkerNetwork        string
	// RedisPasswordFile is the host path to a file containing the Redis
	// password. When set, worker containers receive a bind-mounted copy
	// instead of a plaintext REDIS_PASSWORD environment variable.
	RedisPasswordFile string
	// WorkerMemoryBytes is the memory limit for spawned worker containers.
	// Zero means no limit.
	WorkerMemoryBytes int64
	// WorkerPidsLimit is the PID limit for spawned worker containers.
	// Zero means no limit.
	WorkerPidsLimit int64
	// MaxContainers is the maximum number of worker containers.
	// Zero uses the default (100).
	MaxContainers int
}

// Worker holds configuration for the worker component.
type Worker struct {
	Redis          Redis
	CommandChannel string
	ContainerName  string
	ReportsQueue   string
}

// Monitor holds configuration for the monitor component.
type Monitor struct {
	Redis             Redis
	CheckInterval     time.Duration
	MaxSilenceSeconds int64
	SlotIdleTimeout   int64
	TasksQueue        string
}

func loadRedis() (Redis, error) {
	password := envOrDefault("REDIS_PASSWORD", "")

	if password == "" {
		var err error

		password, err = loadPasswordFile()
		if err != nil {
			return Redis{}, err
		}
	}

	return Redis{
		Host:     envOrDefault("REDIS_HOST", "localhost"),
		Port:     envOrDefault("REDIS_PORT", "6379"),
		Password: password,
	}, nil
}

// loadPasswordFile reads a password from the file pointed to by
// REDIS_PASSWORD_FILE. Returns empty string if the env var is unset,
// and an error if the file cannot be read.
func loadPasswordFile() (string, error) {
	path := os.Getenv("REDIS_PASSWORD_FILE")
	if path == "" {
		return "", nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return "", errors.Wrapf(errdefs.ErrInvalidConfig,
			"failed to read REDIS_PASSWORD_FILE %q: %v", path, err)
	}

	return strings.TrimSpace(string(data)), nil
}

// LoadManagerFromEnv creates Manager config from environment variables.
func LoadManagerFromEnv() (Manager, error) {
	const defaultMaxSlots = 10

	redisCfg, err := loadRedis()
	if err != nil {
		return Manager{}, err
	}

	maxSlots, err := envInt("MAX_SLOTS_PER_CONTAINER", defaultMaxSlots)
	if err != nil {
		return Manager{}, err
	}

	workerMemory, err := envInt("WORKER_MEMORY_BYTES", 0)
	if err != nil {
		return Manager{}, err
	}

	workerPids, err := envInt("WORKER_PIDS_LIMIT", 0)
	if err != nil {
		return Manager{}, err
	}

	maxContainers, err := envInt("MAX_CONTAINERS", 0)
	if err != nil {
		return Manager{}, err
	}

	return Manager{
		Redis:                redisCfg,
		MaxSlotsPerContainer: maxSlots,
		WorkerImage:          envOrDefault("WORKER_IMAGE", "redtailfox:latest"),
		ContainerNamePrefix:  envOrDefault("WORKER_CONTAINER_PREFIX", "fox_worker"),
		CommandChannelPrefix: envOrDefault("COMMAND_CHANNEL_PREFIX", "COMMAND_CHANNEL"),
		TasksQueue:           envOrDefault("WORKER_TASKS_LIST", "manager_tasks"),
		ReportsQueue:         envOrDefault("WORKER_REPORTS_CHANNEL", "worker_reports"),
		DBWriteQueue:         envOrDefault("DB_WRITE_QUEUE", "db_write_requests"),
		WorkerNetwork:        envOrDefault("WORKER_NETWORK", ""),
		RedisPasswordFile:    os.Getenv("REDIS_PASSWORD_FILE"),
		WorkerMemoryBytes:    int64(workerMemory),
		WorkerPidsLimit:      int64(workerPids),
		MaxContainers:        maxContainers,
	}, nil
}

// LoadWorkerFromEnv creates Worker config from environment variables.
func LoadWorkerFromEnv() (Worker, error) {
	redisCfg, err := loadRedis()
	if err != nil {
		return Worker{}, err
	}

	return Worker{
		Redis:          redisCfg,
		CommandChannel: envOrDefault("COMMAND_CHANNEL", "worker_commands"),
		ContainerName:  envOrDefault("CONTAINER_NAME", "unknown_container"),
		ReportsQueue:   envOrDefault("WORKER_REPORTS_CHANNEL", "worker_reports"),
	}, nil
}

// LoadMonitorFromEnv creates Monitor config from environment variables.
func LoadMonitorFromEnv() (Monitor, error) {
	const (
		defaultCheckInterval = 10
		defaultMaxSilence    = 500
		defaultSlotIdle      = 600
	)

	interval, err := envInt("CHECK_INTERVAL", defaultCheckInterval)
	if err != nil {
		return Monitor{}, err
	}

	maxSilence, err := envInt("MAX_SILENCE_SECONDS", defaultMaxSilence)
	if err != nil {
		return Monitor{}, err
	}

	slotIdle, err := envInt("SLOT_IDLE_TIMEOUT", defaultSlotIdle)
	if err != nil {
		return Monitor{}, err
	}

	redisCfg, err := loadRedis()
	if err != nil {
		return Monitor{}, err
	}

	return Monitor{
		Redis:             redisCfg,
		CheckInterval:     time.Duration(interval) * time.Second,
		MaxSilenceSeconds: int64(maxSilence),
		SlotIdleTimeout:   int64(slotIdle),
		TasksQueue:        envOrDefault("WORKER_TASKS_LIST", "manager_tasks"),
	}, nil
}

func envOrDefault(key, fallback string) string {
	val, ok := os.LookupEnv(key)
	if !ok || val == "" {
		return fallback
	}

	return val
}

// envInt reads an integer environment variable. Returns fallback when the
// variable is unset or empty, and an error when the value is not a valid integer.
func envInt(key string, fallback int) (int, error) {
	val, ok := os.LookupEnv(key)
	if !ok || val == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(val)
	if err != nil {
		return 0, errors.Wrapf(errdefs.ErrInvalidConfig,
			"env var %s has invalid integer value %q", key, val)
	}

	return parsed, nil
}
