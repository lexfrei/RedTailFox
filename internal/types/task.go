package types

import "encoding/json"

// Task represents a command message sent via Redis queues.
type Task struct {
	// Command is the action to perform.
	Command Command `json:"command"`

	// SlotID identifies the target slot.
	SlotID int `json:"slotID"`

	// ContainerName is the target container (used for container-level commands).
	ContainerName string `json:"containerName,omitempty"`

	// Config holds slot-specific configuration as raw JSON.
	Config json.RawMessage `json:"config,omitempty"`

	// InitiatedBy indicates who triggered this task (e.g., "monitor", "user").
	InitiatedBy string `json:"initiatedBy,omitempty"`
}
