package model

import "regexp"

// Redis key prefixes and names shared between manager, worker, and monitor.
const (
	// HeartbeatKeyPrefix is used by the worker to publish heartbeats and by
	// the monitor to read them.
	HeartbeatKeyPrefix = "hb:container:"

	// ActiveContainersKey is the Redis set of currently active container names,
	// maintained by the manager and read by the monitor.
	ActiveContainersKey = "manager:active_containers"
)

// ContainerNameRe validates OCI container names. Used by both the manager
// (for prefix validation) and the monitor (for active-set validation).
var ContainerNameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)
