package manager_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/lexfrei/RedTailFox/internal/manager"
)

func setupRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()

	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})

	return srv, rdb
}

func TestRegisterSlot(t *testing.T) {
	_, rdb := setupRedis(t)
	ctx := context.Background()
	state := manager.NewState(rdb, 10)

	state.RegisterSlot(ctx, 1, "fox_worker_1")

	container, err := state.GetSlotContainer(ctx, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if container != "fox_worker_1" {
		t.Errorf("expected fox_worker_1, got %s", container)
	}
}

func TestUnregisterSlot(t *testing.T) {
	_, rdb := setupRedis(t)
	ctx := context.Background()
	state := manager.NewState(rdb, 10)

	state.RegisterSlot(ctx, 1, "fox_worker_1")
	containerName, err := state.UnregisterSlot(ctx, 1)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if containerName != "fox_worker_1" {
		t.Errorf("expected fox_worker_1, got %s", containerName)
	}

	// Verify slot is gone.
	_, err = state.GetSlotContainer(ctx, 1)
	if err == nil {
		t.Error("expected error for unregistered slot")
	}
}

func TestPickContainer_HasFreeSlots(t *testing.T) {
	_, rdb := setupRedis(t)
	ctx := context.Background()
	state := manager.NewState(rdb, 2)

	state.RegisterSlot(ctx, 1, "fox_worker_1")

	container, err := state.PickContainer(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if container != "fox_worker_1" {
		t.Errorf("expected fox_worker_1, got %s", container)
	}
}

func TestPickContainer_AllFull(t *testing.T) {
	_, rdb := setupRedis(t)
	ctx := context.Background()
	state := manager.NewState(rdb, 1)

	state.RegisterSlot(ctx, 1, "fox_worker_1")

	container, err := state.PickContainer(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if container != "" {
		t.Errorf("expected empty string when all full, got %s", container)
	}
}

func TestSaveAndGetSlotConfig(t *testing.T) {
	_, rdb := setupRedis(t)
	ctx := context.Background()
	state := manager.NewState(rdb, 10)

	cfg := []byte(`{"bot":{"interval":300}}`)

	if err := state.SaveSlotConfig(ctx, 1, cfg); err != nil {
		t.Fatalf("unexpected error saving config: %v", err)
	}

	got, err := state.GetSlotConfig(ctx, 1)
	if err != nil {
		t.Fatalf("unexpected error getting config: %v", err)
	}

	if string(got) != string(cfg) {
		t.Errorf("expected %s, got %s", cfg, got)
	}
}

func TestGetSlotConfig_NotFound(t *testing.T) {
	_, rdb := setupRedis(t)
	ctx := context.Background()
	state := manager.NewState(rdb, 10)

	_, err := state.GetSlotConfig(ctx, 999)
	if err == nil {
		t.Error("expected error for missing config")
	}
}

func TestNextContainerIndex(t *testing.T) {
	_, rdb := setupRedis(t)
	ctx := context.Background()
	state := manager.NewState(rdb, 10)

	idx1, err := state.NextContainerIndex(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	idx2, err := state.NextContainerIndex(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if idx1 >= idx2 {
		t.Errorf("expected incrementing indices, got %d and %d", idx1, idx2)
	}
}

func TestContainerSlotCount(t *testing.T) {
	_, rdb := setupRedis(t)
	ctx := context.Background()
	state := manager.NewState(rdb, 10)

	state.RegisterSlot(ctx, 1, "fox_worker_1")
	state.RegisterSlot(ctx, 2, "fox_worker_1")

	count, err := state.ContainerSlotCount(ctx, "fox_worker_1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 2 {
		t.Errorf("expected 2 slots, got %d", count)
	}
}

func TestActiveContainers(t *testing.T) {
	_, rdb := setupRedis(t)
	ctx := context.Background()
	state := manager.NewState(rdb, 10)

	state.RegisterSlot(ctx, 1, "fox_worker_1")
	state.RegisterSlot(ctx, 2, "fox_worker_2")

	active, err := state.ActiveContainers(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(active) != 2 {
		t.Errorf("expected 2 active containers, got %d", len(active))
	}
}

func TestRemoveContainer(t *testing.T) {
	_, rdb := setupRedis(t)
	ctx := context.Background()
	state := manager.NewState(rdb, 10)

	state.RegisterSlot(ctx, 1, "fox_worker_1")
	state.RemoveContainer(ctx, "fox_worker_1")

	active, err := state.ActiveContainers(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(active) != 0 {
		t.Errorf("expected 0 active containers, got %d", len(active))
	}
}
