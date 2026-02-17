package container

import (
	"context"
	"log/slog"
	"time"

	"github.com/cockroachdb/errors"
	apitypes "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	rtferrors "github.com/Dark-F0X/RedTailFox/internal/errdefs"
)

// OCIRuntime implements Runtime using an OCI-compatible container engine.
type OCIRuntime struct {
	cli *client.Client
	log *slog.Logger
}

// NewOCIRuntime creates a new OCI runtime using the default environment connection.
// It validates the connection by pinging the daemon before returning.
func NewOCIRuntime(ctx context.Context, log *slog.Logger) (*OCIRuntime, error) {
	cli, err := client.New(client.FromEnv)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to container runtime")
	}

	if _, err := cli.ServerVersion(ctx, client.ServerVersionOptions{}); err != nil {
		return nil, errors.Wrap(err, "verifying container runtime connection")
	}

	return &OCIRuntime{cli: cli, log: log}, nil
}

// Run creates and starts a new container.
func (r *OCIRuntime) Run(ctx context.Context, opts *RunOptions) (Container, error) {
	hostCfg := &apitypes.HostConfig{
		RestartPolicy: apitypes.RestartPolicy{
			Name: apitypes.RestartPolicyMode(opts.RestartPolicy),
		},
	}

	if opts.Network != "" {
		hostCfg.NetworkMode = apitypes.NetworkMode(opts.Network)
	}

	createOpts := client.ContainerCreateOptions{
		Name:  opts.Name,
		Image: opts.Image,
		Config: &apitypes.Config{
			Cmd: opts.Command,
			Env: makeEnvList(opts.Env),
		},
		HostConfig: hostCfg,
	}

	result, err := r.cli.ContainerCreate(ctx, createOpts)
	if err != nil {
		return Container{}, errors.Wrap(err, "creating container")
	}

	startOpts := client.ContainerStartOptions{}
	if _, err := r.cli.ContainerStart(ctx, result.ID, startOpts); err != nil {
		return Container{}, errors.Wrap(err, "starting container")
	}

	r.log.Info("container started", "name", opts.Name, "id", result.ID[:12])

	return Container{ID: result.ID, Name: opts.Name}, nil
}

// Stop gracefully stops a container by name.
func (r *OCIRuntime) Stop(ctx context.Context, name string, timeout time.Duration) error {
	timeoutSec := int(timeout.Seconds())

	stopOpts := client.ContainerStopOptions{
		Timeout: &timeoutSec,
	}

	if _, err := r.cli.ContainerStop(ctx, name, stopOpts); err != nil {
		return errors.Wrap(err, "stopping container")
	}

	r.log.Info("container stopped", "name", name)

	return nil
}

// Remove deletes a stopped container by name.
func (r *OCIRuntime) Remove(ctx context.Context, name string) error {
	removeOpts := client.ContainerRemoveOptions{}

	if _, err := r.cli.ContainerRemove(ctx, name, removeOpts); err != nil {
		return errors.Wrap(err, "removing container")
	}

	r.log.Info("container removed", "name", name)

	return nil
}

// List returns running containers matching the given name prefix.
func (r *OCIRuntime) List(ctx context.Context, namePrefix string) ([]Container, error) {
	if r.cli == nil {
		return nil, errors.Wrap(rtferrors.ErrRuntimeUnavailable, "listing containers")
	}

	filters := make(client.Filters)
	filters.Add("name", namePrefix)

	listOpts := client.ContainerListOptions{
		All:     false,
		Filters: filters,
	}

	listed, err := r.cli.ContainerList(ctx, listOpts)
	if err != nil {
		return nil, errors.Wrap(err, "listing containers")
	}

	// Server-side name filter uses substring match, so apply prefix filter
	// client-side as a safety measure.
	return filterByPrefix(listed.Items, namePrefix), nil
}

func makeEnvList(env map[string]string) []string {
	result := make([]string, 0, len(env))
	for key, val := range env {
		result = append(result, key+"="+val)
	}

	return result
}
