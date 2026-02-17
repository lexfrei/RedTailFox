package config_test

import (
	"testing"
	"time"

	"github.com/Dark-F0X/RedTailFox/internal/config"
)

func TestLoadManagerFromEnv_Defaults(t *testing.T) {
	t.Setenv("REDIS_HOST", "")
	t.Setenv("REDIS_PORT", "")
	t.Setenv("REDIS_PASSWORD", "")
	t.Setenv("MAX_SLOTS_PER_CONTAINER", "")
	t.Setenv("WORKER_IMAGE", "")
	t.Setenv("WORKER_CONTAINER_PREFIX", "")

	cfg := config.LoadManagerFromEnv()

	if cfg.Redis.Host != "localhost" {
		t.Errorf("expected default host localhost, got %s", cfg.Redis.Host)
	}

	if cfg.Redis.Port != "6379" {
		t.Errorf("expected default port 6379, got %s", cfg.Redis.Port)
	}

	if cfg.MaxSlotsPerContainer != 10 {
		t.Errorf("expected default max slots 10, got %d", cfg.MaxSlotsPerContainer)
	}

	if cfg.WorkerImage != "fox_worker:latest" {
		t.Errorf("expected default worker image, got %s", cfg.WorkerImage)
	}

	if cfg.ContainerNamePrefix != "fox_worker" {
		t.Errorf("expected default prefix, got %s", cfg.ContainerNamePrefix)
	}
}

func TestLoadManagerFromEnv_CustomValues(t *testing.T) {
	t.Setenv("REDIS_HOST", "redis.example.com")
	t.Setenv("REDIS_PORT", "6380")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("MAX_SLOTS_PER_CONTAINER", "20")
	t.Setenv("WORKER_IMAGE", "custom:v1")
	t.Setenv("WORKER_CONTAINER_PREFIX", "custom_worker")

	cfg := config.LoadManagerFromEnv()

	if cfg.Redis.Host != "redis.example.com" {
		t.Errorf("expected custom host, got %s", cfg.Redis.Host)
	}

	if cfg.Redis.Port != "6380" {
		t.Errorf("expected custom port, got %s", cfg.Redis.Port)
	}

	if cfg.Redis.Password != "secret" {
		t.Errorf("expected custom password, got %s", cfg.Redis.Password)
	}

	if cfg.MaxSlotsPerContainer != 20 {
		t.Errorf("expected 20 max slots, got %d", cfg.MaxSlotsPerContainer)
	}

	if cfg.WorkerImage != "custom:v1" {
		t.Errorf("expected custom image, got %s", cfg.WorkerImage)
	}
}

func TestLoadWorkerFromEnv_Defaults(t *testing.T) {
	t.Setenv("COMMAND_CHANNEL", "")
	t.Setenv("EVENT_CHANNEL", "")
	t.Setenv("CONTAINER_NAME", "")

	cfg := config.LoadWorkerFromEnv()

	if cfg.CommandChannel != "worker_commands" {
		t.Errorf("expected default command channel, got %s", cfg.CommandChannel)
	}

	if cfg.EventChannel != "events" {
		t.Errorf("expected default event channel, got %s", cfg.EventChannel)
	}

	if cfg.ContainerName != "unknown_container" {
		t.Errorf("expected default container name, got %s", cfg.ContainerName)
	}
}

func TestLoadMonitorFromEnv_Defaults(t *testing.T) {
	t.Setenv("CHECK_INTERVAL", "")
	t.Setenv("MAX_SILENCE_SECONDS", "")
	t.Setenv("SLOT_IDLE_TIMEOUT", "")

	cfg := config.LoadMonitorFromEnv()

	expectedInterval := 10 * time.Second
	if cfg.CheckInterval != expectedInterval {
		t.Errorf("expected check interval %v, got %v", expectedInterval, cfg.CheckInterval)
	}

	if cfg.MaxSilenceSeconds != 500 {
		t.Errorf("expected max silence 500, got %d", cfg.MaxSilenceSeconds)
	}

	if cfg.SlotIdleTimeout != 600 {
		t.Errorf("expected slot idle timeout 600, got %d", cfg.SlotIdleTimeout)
	}
}

func TestRedisOptions(t *testing.T) {
	cfg := config.Redis{
		Host:     "myhost",
		Port:     "6380",
		Password: "pass",
	}

	opts := cfg.Options()

	expectedAddr := "myhost:6380"
	if opts.Addr != expectedAddr {
		t.Errorf("expected addr %s, got %s", expectedAddr, opts.Addr)
	}

	if opts.Password != "pass" {
		t.Errorf("expected password pass, got %s", opts.Password)
	}
}
