// Package errdefs defines sentinel errors for the RedTailFox orchestrator.
package errdefs

import "github.com/cockroachdb/errors"

// Sentinel errors for slot operations.
var (
	// ErrSlotAlreadyRunning indicates an attempt to start a slot that is already active.
	ErrSlotAlreadyRunning = errors.New("slot already running")

	// ErrSlotNotFound indicates a slot lookup failed.
	ErrSlotNotFound = errors.New("slot not found")
)

// Sentinel errors for container operations.
var (
	// ErrContainerNotFound indicates a container lookup failed.
	ErrContainerNotFound = errors.New("container not found")

	// ErrRuntimeUnavailable indicates the container runtime is not accessible.
	ErrRuntimeUnavailable = errors.New("container runtime unavailable")
)

// Sentinel errors for configuration.
var (
	// ErrConfigNotFound indicates a slot config is missing from Redis.
	ErrConfigNotFound = errors.New("slot config not found in redis")
)

// Sentinel errors for commands.
var (
	// ErrUnknownCommand indicates an unrecognized task command.
	ErrUnknownCommand = errors.New("unknown command")
)

// Sentinel errors for configuration validation.
var (
	// ErrInvalidConfig indicates a configuration value is invalid.
	ErrInvalidConfig = errors.New("invalid configuration")
)
