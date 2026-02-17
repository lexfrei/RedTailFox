package types

// WorkerReport is sent by a worker to the manager to report slot status changes.
type WorkerReport struct {
	// SlotID identifies the reporting slot.
	SlotID int `json:"slotID"`

	// Status is the current slot status (e.g., "started", "stopped", "error").
	Status string `json:"status"`

	// ContainerName is the name of the container hosting this slot.
	ContainerName string `json:"containerName,omitempty"`

	// ErrorText provides details when status is "error".
	ErrorText string `json:"errorText,omitempty"`

	// InitiatedBy indicates who triggered the status change.
	InitiatedBy string `json:"initiatedBy,omitempty"`
}

// DBWriteEvent is published to the database write queue for persistence.
type DBWriteEvent struct {
	// SlotID identifies the slot.
	SlotID int `json:"slotID"`

	// Status is the slot status to record.
	Status string `json:"status"`

	// InitiatedBy indicates the originator of this event.
	InitiatedBy string `json:"initiatedBy"`

	// Timestamp is the Unix epoch when this event occurred.
	Timestamp int64 `json:"timestamp"`

	// ErrorText provides details for error events.
	ErrorText string `json:"errorText,omitempty"`
}
