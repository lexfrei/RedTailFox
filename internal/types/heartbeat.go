package types

// Heartbeat is the health check payload published by each worker container.
type Heartbeat struct {
	// Container is the name of the reporting container.
	Container string `json:"container"`

	// Timestamp is the Unix epoch when this heartbeat was created.
	Timestamp int64 `json:"ts"`

	// Slots contains the state of each slot in this container.
	Slots []SlotHeartbeat `json:"slots"`
}

// SlotHeartbeat represents the health state of a single slot within a heartbeat.
type SlotHeartbeat struct {
	// SlotID identifies the slot.
	SlotID int `json:"slotID"`

	// Running indicates whether the slot goroutine is active.
	Running bool `json:"running"`

	// Status is the current slot status string.
	Status string `json:"status"`

	// LastActive is the Unix epoch of the last status transition.
	LastActive int64 `json:"lastActive"`

	// Timestamp is the Unix epoch when this snapshot was taken.
	Timestamp int64 `json:"ts"`
}
