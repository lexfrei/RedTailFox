// Package manager implements the RedTailFox task manager.
package manager

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"

	rtferrors "github.com/Dark-F0X/RedTailFox/internal/errdefs"
)

// Redis key constants for state management.
const (
	slotToContainerKey     = "manager:slot_to_container"
	containerInfoKeyPrefix = "manager:container:"
	activeContainersKey    = "manager:active_containers"
	containerCounterKey    = "manager:container_counter"
	slotConfigKeyPrefix    = "manager:config:slot:"
	containerChannelsKey   = "manager:container_channels"
)

// State manages the Redis-backed state for slot-to-container mappings.
type State struct {
	rdb      *redis.Client
	maxSlots int
}

// NewState creates a new State manager.
func NewState(rdb *redis.Client, maxSlots int) *State {
	return &State{rdb: rdb, maxSlots: maxSlots}
}

// RegisterSlot assigns a slot to a container in Redis atomically via pipeline.
func (s *State) RegisterSlot(ctx context.Context, slotID int, containerName string) error {
	sid := strconv.Itoa(slotID)

	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, slotToContainerKey, sid, containerName)
	pipe.SAdd(ctx, containerSlotsKey(containerName), sid)
	pipe.SAdd(ctx, activeContainersKey, containerName)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return errors.Wrap(err, "registering slot in pipeline")
	}

	return nil
}

// UnregisterSlot removes a slot from its container and returns the container name.
func (s *State) UnregisterSlot(ctx context.Context, slotID int) (string, error) {
	sid := strconv.Itoa(slotID)

	containerName, err := s.rdb.HGet(ctx, slotToContainerKey, sid).Result()
	if err != nil {
		return "", errors.Wrap(rtferrors.ErrSlotNotFound, "unregistering slot")
	}

	pipe := s.rdb.TxPipeline()
	pipe.HDel(ctx, slotToContainerKey, sid)
	pipe.SRem(ctx, containerSlotsKey(containerName), sid)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return containerName, errors.Wrap(err, "cleaning up slot state")
	}

	return containerName, nil
}

// GetSlotContainer returns the container name for a given slot.
func (s *State) GetSlotContainer(ctx context.Context, slotID int) (string, error) {
	sid := strconv.Itoa(slotID)

	val, err := s.rdb.HGet(ctx, slotToContainerKey, sid).Result()
	if err != nil {
		return "", errors.Wrap(rtferrors.ErrSlotNotFound, "looking up slot container")
	}

	return val, nil
}

// pickContainerScript atomically finds a container with available capacity.
// KEYS[1] = active containers set.
// ARGV[1] = max slots, ARGV[2] = container info key prefix, ARGV[3] = slots suffix.
//
//nolint:gochecknoglobals,dupword // Pre-compiled Lua script; "end" closes both the if-block and the for-loop.
var pickContainerScript = redis.NewScript(`
local members = redis.call('SMEMBERS', KEYS[1])
local max = tonumber(ARGV[1])
local prefix = ARGV[2]
local suffix = ARGV[3]
for _, name in ipairs(members) do
    local count = redis.call('SCARD', prefix .. name .. suffix)
    if count < max then
        return name
    end
end
return ''
`)

// PickContainer atomically finds a container with available slot capacity.
// Returns empty string if all containers are full.
func (s *State) PickContainer(ctx context.Context) (string, error) {
	result, err := pickContainerScript.Run(
		ctx, s.rdb,
		[]string{activeContainersKey},
		s.maxSlots, containerInfoKeyPrefix, ":slots",
	).Text()
	if err != nil {
		return "", errors.Wrap(err, "picking container atomically")
	}

	return result, nil
}

// SaveSlotConfig stores slot configuration in Redis for restart capability.
func (s *State) SaveSlotConfig(ctx context.Context, slotID int, cfg []byte) error {
	key := fmt.Sprintf("%s%d", slotConfigKeyPrefix, slotID)

	if err := s.rdb.Set(ctx, key, cfg, 0).Err(); err != nil {
		return errors.Wrap(err, "saving slot config")
	}

	return nil
}

// GetSlotConfig retrieves stored slot configuration from Redis.
func (s *State) GetSlotConfig(ctx context.Context, slotID int) ([]byte, error) {
	key := fmt.Sprintf("%s%d", slotConfigKeyPrefix, slotID)

	val, err := s.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return nil, errors.Wrap(rtferrors.ErrConfigNotFound, "getting slot config")
	}

	return val, nil
}

// NextContainerIndex atomically increments and returns the container counter.
func (s *State) NextContainerIndex(ctx context.Context) (int64, error) {
	idx, err := s.rdb.Incr(ctx, containerCounterKey).Result()
	if err != nil {
		return 0, errors.Wrap(err, "incrementing container counter")
	}

	return idx, nil
}

// ContainerSlotCount returns the number of slots in a container.
func (s *State) ContainerSlotCount(ctx context.Context, containerName string) (int64, error) {
	count, err := s.rdb.SCard(ctx, containerSlotsKey(containerName)).Result()
	if err != nil {
		return 0, errors.Wrap(err, "counting container slots")
	}

	return count, nil
}

// ActiveContainers returns the set of all active container names.
func (s *State) ActiveContainers(ctx context.Context) ([]string, error) {
	members, err := s.rdb.SMembers(ctx, activeContainersKey).Result()
	if err != nil {
		return nil, errors.Wrap(err, "listing active containers")
	}

	return members, nil
}

// ContainerSlots returns all slot IDs in a container.
func (s *State) ContainerSlots(ctx context.Context, containerName string) ([]string, error) {
	slots, err := s.rdb.SMembers(ctx, containerSlotsKey(containerName)).Result()
	if err != nil {
		return nil, errors.Wrap(err, "listing container slots")
	}

	return slots, nil
}

// SetContainerChannel stores the command channel for a container.
func (s *State) SetContainerChannel(ctx context.Context, containerName, channel string) error {
	err := s.rdb.HSet(ctx, containerChannelsKey, containerName, channel).Err()
	if err != nil {
		return errors.Wrap(err, "storing container channel")
	}

	return nil
}

// GetContainerChannel retrieves the command channel for a container.
func (s *State) GetContainerChannel(ctx context.Context, containerName string) (string, error) {
	val, err := s.rdb.HGet(ctx, containerChannelsKey, containerName).Result()
	if err != nil {
		return "", errors.Wrap(err, "looking up container channel")
	}

	return val, nil
}

// removeContainerScript atomically removes all state for a container:
// the slot set, active membership, command channel, and orphaned slot-to-container mappings.
// KEYS[1] = slots set key, KEYS[2] = active containers set, KEYS[3] = slot-to-container hash,
// KEYS[4] = container channels hash.
// ARGV[1] = container name.
//
//nolint:gochecknoglobals // Pre-compiled Lua script for atomic container state removal.
var removeContainerScript = redis.NewScript(`
local slotsKey = KEYS[1]
local activeKey = KEYS[2]
local s2cKey = KEYS[3]
local channelsKey = KEYS[4]
local containerName = ARGV[1]

local slots = redis.call('SMEMBERS', slotsKey)
for _, sid in ipairs(slots) do
    redis.call('HDEL', s2cKey, sid)
end
redis.call('DEL', slotsKey)
redis.call('SREM', activeKey, containerName)
redis.call('HDEL', channelsKey, containerName)
return 1
`)

// RemoveContainer atomically removes all state for a container: the slot set,
// active membership, command channel, and orphaned slot-to-container mappings.
func (s *State) RemoveContainer(ctx context.Context, containerName string) error {
	err := removeContainerScript.Run(
		ctx, s.rdb,
		[]string{
			containerSlotsKey(containerName),
			activeContainersKey,
			slotToContainerKey,
			containerChannelsKey,
		},
		containerName,
	).Err()
	if err != nil {
		return errors.Wrap(err, "removing container state")
	}

	return nil
}

// SlotExists checks if a slot is currently registered.
func (s *State) SlotExists(ctx context.Context, slotID int) (bool, error) {
	sid := strconv.Itoa(slotID)

	exists, err := s.rdb.HExists(ctx, slotToContainerKey, sid).Result()
	if err != nil {
		return false, errors.Wrap(err, "checking slot existence")
	}

	return exists, nil
}

func containerSlotsKey(containerName string) string {
	return containerInfoKeyPrefix + containerName + ":slots"
}
