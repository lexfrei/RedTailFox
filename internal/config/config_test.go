package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/Dark-F0X/RedTailFox/internal/config"
	"github.com/Dark-F0X/RedTailFox/internal/errdefs"
)

func TestLoadManagerFromEnv_Defaults(t *testing.T) {
	t.Setenv("REDIS_HOST", "")
	t.Setenv("REDIS_PORT", "")
	t.Setenv("REDIS_PASSWORD", "")
	t.Setenv("REDIS_PASSWORD_FILE", "")
	t.Setenv("MAX_SLOTS_PER_CONTAINER", "")
	t.Setenv("WORKER_IMAGE", "")
	t.Setenv("WORKER_CONTAINER_PREFIX", "")

	cfg, err := config.LoadManagerFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Redis.Host != "localhost" {
		t.Errorf("expected default host localhost, got %s", cfg.Redis.Host)
	}

	if cfg.Redis.Port != "6379" {
		t.Errorf("expected default port 6379, got %s", cfg.Redis.Port)
	}

	if cfg.MaxSlotsPerContainer != 10 {
		t.Errorf("expected default max slots 10, got %d", cfg.MaxSlotsPerContainer)
	}

	if cfg.WorkerImage != "redtailfox:latest" {
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

	cfg, err := config.LoadManagerFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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

func TestLoadManagerFromEnv_InvalidInteger(t *testing.T) {
	t.Setenv("MAX_SLOTS_PER_CONTAINER", "notanumber")

	_, err := config.LoadManagerFromEnv()
	if err == nil {
		t.Fatal("expected error for invalid integer env var")
	}

	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestLoadWorkerFromEnv_Defaults(t *testing.T) {
	t.Setenv("COMMAND_CHANNEL", "")
	t.Setenv("CONTAINER_NAME", "")
	t.Setenv("REDIS_PASSWORD_FILE", "")

	cfg, err := config.LoadWorkerFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.CommandChannel != "worker_commands" {
		t.Errorf("expected default command channel, got %s", cfg.CommandChannel)
	}

	if cfg.ContainerName != "unknown_container" {
		t.Errorf("expected default container name, got %s", cfg.ContainerName)
	}
}

func TestLoadMonitorFromEnv_Defaults(t *testing.T) {
	t.Setenv("CHECK_INTERVAL", "")
	t.Setenv("MAX_SILENCE_SECONDS", "")
	t.Setenv("SLOT_IDLE_TIMEOUT", "")

	cfg, err := config.LoadMonitorFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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

	if opts.DialTimeout == 0 {
		t.Error("expected non-zero dial timeout")
	}

	if opts.ReadTimeout == 0 {
		t.Error("expected non-zero read timeout")
	}

	if opts.WriteTimeout == 0 {
		t.Error("expected non-zero write timeout")
	}
}

func TestRedisValidate_PortBoundary(t *testing.T) {
	tests := []struct {
		name    string
		port    string
		wantErr bool
	}{
		{"port 0", "0", true},
		{"port 1", "1", false},
		{"port 65535", "65535", false},
		{"port 65536", "65536", true},
		{"port empty", "", true},
		{"port letters", "abc", true},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			cfg := config.Redis{Host: "localhost", Port: tst.port}
			err := cfg.Validate()

			if tst.wantErr && err == nil {
				t.Errorf("expected error for port %q", tst.port)
			}

			if !tst.wantErr && err != nil {
				t.Errorf("unexpected error for port %q: %v", tst.port, err)
			}
		})
	}
}

func TestLoadRedisPasswordFile(t *testing.T) {
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "redis-password")

	err := os.WriteFile(secretFile, []byte("file-secret\n"), 0o600)
	if err != nil {
		t.Fatalf("failed to write secret file: %v", err)
	}

	// REDIS_PASSWORD takes precedence over REDIS_PASSWORD_FILE.
	t.Setenv("REDIS_PASSWORD", "")
	t.Setenv("REDIS_PASSWORD_FILE", secretFile)

	cfg, loadErr := config.LoadManagerFromEnv()
	if loadErr != nil {
		t.Fatalf("unexpected error: %v", loadErr)
	}

	if cfg.Redis.Password != "file-secret" {
		t.Errorf("expected password from file, got %q", cfg.Redis.Password)
	}
}

func TestLoadRedisPasswordEnvTakesPrecedence(t *testing.T) {
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "redis-password")

	err := os.WriteFile(secretFile, []byte("file-secret\n"), 0o600)
	if err != nil {
		t.Fatalf("failed to write secret file: %v", err)
	}

	t.Setenv("REDIS_PASSWORD", "env-secret")
	t.Setenv("REDIS_PASSWORD_FILE", secretFile)

	cfg, loadErr := config.LoadManagerFromEnv()
	if loadErr != nil {
		t.Fatalf("unexpected error: %v", loadErr)
	}

	if cfg.Redis.Password != "env-secret" {
		t.Errorf("expected env password to take precedence, got %q", cfg.Redis.Password)
	}
}

func TestLoadRedisPasswordFile_Missing(t *testing.T) {
	t.Setenv("REDIS_PASSWORD", "")
	t.Setenv("REDIS_PASSWORD_FILE", "/nonexistent/path/to/secret")

	_, err := config.LoadManagerFromEnv()
	if err == nil {
		t.Fatal("expected error for missing REDIS_PASSWORD_FILE")
	}

	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig, got %v", err)
	}
}
