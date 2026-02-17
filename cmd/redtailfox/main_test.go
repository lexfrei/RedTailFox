package main

import (
	"testing"
	"time"

	"github.com/Dark-F0X/RedTailFox/internal/config"
	"github.com/Dark-F0X/RedTailFox/internal/errdefs"

	"github.com/cockroachdb/errors"
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
