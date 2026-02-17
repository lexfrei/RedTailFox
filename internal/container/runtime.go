// Package container provides a vendor-neutral interface for managing OCI containers.
package container

import (
	"context"
	"time"
)

// Container represents a running container instance.
type Container struct {
	// ID is the unique container identifier.
	ID string

	// Name is the human-readable container name.
	Name string
}

// RunOptions specifies parameters for starting a new container.
type RunOptions struct {
	// Image is the container image reference (e.g., "fox_worker:latest").
	Image string

	// Name is the desired container name.
	Name string

	// Command overrides the container entrypoint arguments (e.g., ["worker"]).
	Command []string

	// Env is a map of environment variables to pass to the container.
	Env map[string]string

	// RestartPolicy defines the container restart behavior (e.g., "unless-stopped").
	RestartPolicy string

	// Network is the container network to attach to (e.g., compose project network).
	Network string

	// Binds is a list of host:container bind mounts (e.g., "/host/path:/container/path:ro").
	Binds []string

	// MemoryBytes limits the container's memory usage in bytes.
	// Zero means no limit (not recommended for untrusted workloads).
	MemoryBytes int64

	// PidsLimit limits the number of processes in the container.
	// Zero means no limit. Set to prevent fork bombs.
	PidsLimit int64

	// ReadOnly mounts the container's root filesystem as read-only.
	ReadOnly bool

	// SecurityOpt is a list of security options (e.g., "no-new-privileges:true").
	SecurityOpt []string
}

// Runtime defines operations for managing OCI-compatible containers.
// Implementations must be safe for concurrent use by multiple goroutines.
type Runtime interface {
	// Run creates and starts a new container with the given options.
	Run(ctx context.Context, opts *RunOptions) (Container, error)

	// Stop gracefully stops a container by name within the given timeout.
	Stop(ctx context.Context, name string, timeout time.Duration) error

	// Remove deletes a stopped container by name.
	Remove(ctx context.Context, name string) error

	// List returns all running containers whose name starts with the given prefix.
	List(ctx context.Context, namePrefix string) ([]Container, error)
}
