package types

// SlotStatus represents the current state of a worker slot.
type SlotStatus string

const (
	// SlotStatusIdle indicates the slot is waiting for work.
	SlotStatusIdle SlotStatus = "idle"

	// SlotStatusReadPending indicates the slot is reading pending chat.
	SlotStatusReadPending SlotStatus = "read_pending_chat"

	// SlotStatusApprovalPending indicates the slot is awaiting approval.
	SlotStatusApprovalPending SlotStatus = "approval_pending_chat"

	// SlotStatusErrorPending indicates the slot encountered an error in chat.
	SlotStatusErrorPending SlotStatus = "error_pending_chat"

	// SlotStatusDirectCheck indicates the slot is performing a direct check.
	SlotStatusDirectCheck SlotStatus = "direct_check"
)
