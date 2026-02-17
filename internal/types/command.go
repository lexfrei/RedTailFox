// Package types defines shared data structures used across all RedTailFox components.
package types

// Command represents a task command type.
type Command string

const (
	// CommandStart initiates a new slot in a container.
	CommandStart Command = "start"

	// CommandStop terminates a running slot.
	CommandStop Command = "stop"

	// CommandRestartSlot restarts a single slot.
	CommandRestartSlot Command = "restart_slot"

	// CommandRestartContainer restarts an entire container and all its slots.
	CommandRestartContainer Command = "restart_container"

	// CommandRun is an alias for CommandStart.
	CommandRun Command = "run"

	// CommandStartWorker is an alias for CommandStart.
	CommandStartWorker Command = "start_worker"
)
