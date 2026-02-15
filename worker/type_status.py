import enum


class SlotStatus(enum.Enum):
    IDLE = "idle"

    READ_PENDING_CHAT = "read_pending_chat"
    APPROVAL_PENDING_CHAT = "approval_pending_chat"
    ERROR_PENDING_CHAT = "error_pending_chat"

