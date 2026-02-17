package worker_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/model"
	"github.com/Dark-F0X/RedTailFox/internal/worker"
)

const (
	testHeartbeatContainer = "fox_worker_1"
	testHeartbeatKey       = "hb:container:" + testHeartbeatContainer
)

func setupHeartbeat(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()

	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})

	return srv, rdb
}

func TestHeartbeatPublish(t *testing.T) {
	srv, rdb := setupHeartbeat(t)
	ctx := context.Background()

	slot := worker.NewSlot(1, json.RawMessage(`{}`), nil, testLogger())
	slot.Start(context.Background())

	defer slot.Stop()

	slotsFunc := func() []*worker.Slot {
		return []*worker.Slot{slot}
	}

	hbt := worker.NewHeartbeat(rdb, testHeartbeatContainer, slotsFunc, testLogger())
	hbt.Publish(ctx)

	key := testHeartbeatKey
	if !srv.Exists(key) {
		t.Fatal("expected heartbeat key to exist in Redis")
	}

	val, err := srv.Get(key)
	if err != nil {
		t.Fatalf("failed to get heartbeat: %v", err)
	}

	var parsed model.Heartbeat

	err = json.Unmarshal([]byte(val), &parsed)
	if err != nil {
		t.Fatalf("failed to parse heartbeat: %v", err)
	}

	if parsed.Container != testHeartbeatContainer {
		t.Errorf("expected container fox_worker_1, got %s", parsed.Container)
	}

	if len(parsed.Slots) != 1 {
		t.Fatalf("expected 1 slot in heartbeat, got %d", len(parsed.Slots))
	}

	if parsed.Slots[0].SlotID != 1 {
		t.Errorf("expected slot ID 1, got %d", parsed.Slots[0].SlotID)
	}
}

func TestHeartbeatTTL(t *testing.T) {
	srv, rdb := setupHeartbeat(t)
	ctx := context.Background()

	slotsFunc := func() []*worker.Slot { return nil }

	hbt := worker.NewHeartbeat(rdb, testHeartbeatContainer, slotsFunc, testLogger())
	hbt.Publish(ctx)

	key := testHeartbeatKey

	ttl := srv.TTL(key)
	if ttl <= 0 {
		t.Errorf("expected positive TTL, got %v", ttl)
	}
}

func TestHeartbeatMultipleSlots(t *testing.T) {
	srv, rdb := setupHeartbeat(t)
	ctx := context.Background()

	slot1 := worker.NewSlot(1, json.RawMessage(`{}`), nil, testLogger())
	slot2 := worker.NewSlot(2, json.RawMessage(`{}`), nil, testLogger())
	slot1.Start(context.Background())
	slot2.Start(context.Background())

	defer slot1.Stop()
	defer slot2.Stop()

	slotsFunc := func() []*worker.Slot {
		return []*worker.Slot{slot1, slot2}
	}

	hbt := worker.NewHeartbeat(rdb, testHeartbeatContainer, slotsFunc, testLogger())
	hbt.Publish(ctx)

	val, _ := srv.Get(testHeartbeatKey)

	var parsed model.Heartbeat

	err := json.Unmarshal([]byte(val), &parsed)
	if err != nil {
		t.Fatalf("failed to parse heartbeat: %v", err)
	}

	if len(parsed.Slots) != 2 {
		t.Errorf("expected 2 slots, got %d", len(parsed.Slots))
	}
}
