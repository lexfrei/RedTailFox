package main

import (
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/Dark-F0X/RedTailFox/internal/config"
	"github.com/Dark-F0X/RedTailFox/internal/errdefs"
)

func TestValidateMonitorConfig_Valid(t *testing.T) {
	cfg := &config.Monitor{
		MaxSilenceSeconds: 500,
		SlotIdleTimeout:   600,
		CheckInterval:     10 * time.Second,
	}

	if err := validateMonitorConfig(cfg); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidateMonitorConfig_NegativeMaxSilence(t *testing.T) {
	cfg := &config.Monitor{
		MaxSilenceSeconds: -1,
		SlotIdleTimeout:   600,
		CheckInterval:     10 * time.Second,
	}

	err := validateMonitorConfig(cfg)
	if err == nil {
		t.Fatal("expected error for negative MaxSilenceSeconds")
	}

	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestValidateMonitorConfig_ZeroSlotIdleTimeout(t *testing.T) {
	cfg := &config.Monitor{
		MaxSilenceSeconds: 500,
		SlotIdleTimeout:   0,
		CheckInterval:     10 * time.Second,
	}

	err := validateMonitorConfig(cfg)
	if err == nil {
		t.Fatal("expected error for zero SlotIdleTimeout")
	}

	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestValidateMonitorConfig_NegativeCheckInterval(t *testing.T) {
	cfg := &config.Monitor{
		MaxSilenceSeconds: 500,
		SlotIdleTimeout:   600,
		CheckInterval:     -1 * time.Second,
	}

	err := validateMonitorConfig(cfg)
	if err == nil {
		t.Fatal("expected error for negative CheckInterval")
	}

	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig, got %v", err)
	}
}

// validManagerCfg returns a config.Manager with all fields valid.
func validManagerCfg() config.Manager {
	return config.Manager{
		WorkerImage:          "redtailfox:latest",
		MaxSlotsPerContainer: 10,
		ContainerNamePrefix:  "fox_worker",
	}
}

func TestValidateManagerConfig_Valid(t *testing.T) {
	cfg := validManagerCfg()
	if err := validateManagerConfig(&cfg); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidateManagerConfig_EmptyWorkerImage(t *testing.T) {
	cfg := validManagerCfg()
	cfg.WorkerImage = ""

	err := validateManagerConfig(&cfg)
	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for empty WORKER_IMAGE, got %v", err)
	}
}

func TestValidateManagerConfig_NegativeMaxSlots(t *testing.T) {
	cfg := validManagerCfg()
	cfg.MaxSlotsPerContainer = -1

	err := validateManagerConfig(&cfg)
	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for negative slots, got %v", err)
	}
}

func TestValidateManagerConfig_EmptyContainerPrefix(t *testing.T) {
	cfg := validManagerCfg()
	cfg.ContainerNamePrefix = ""

	err := validateManagerConfig(&cfg)
	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for empty prefix, got %v", err)
	}
}

func TestValidateManagerConfig_InvalidContainerPrefix(t *testing.T) {
	cfg := validManagerCfg()
	cfg.ContainerNamePrefix = "invalid prefix!"

	err := validateManagerConfig(&cfg)
	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for invalid prefix, got %v", err)
	}
}

func TestValidateManagerConfig_NegativeMemoryBytes(t *testing.T) {
	cfg := validManagerCfg()
	cfg.WorkerMemoryBytes = -1

	err := validateManagerConfig(&cfg)
	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for negative memory, got %v", err)
	}
}

func TestValidateManagerConfig_NegativePidsLimit(t *testing.T) {
	cfg := validManagerCfg()
	cfg.WorkerPidsLimit = -1

	err := validateManagerConfig(&cfg)
	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for negative pids limit, got %v", err)
	}
}

func TestRun_NoArgs(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"redtailfox"}

	err := run()
	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for missing args, got %v", err)
	}
}

func TestRun_UnknownCommand(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"redtailfox", "bogus"}

	err := run()
	if !errors.Is(err, errdefs.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for unknown command, got %v", err)
	}
}
