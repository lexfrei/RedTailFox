// Package manager implements the RedTailFox task manager.
package manager

import (
	"context"
	"fmt"
	"strconv"
	"time"

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

// registerSlotScript atomically registers a slot if not already assigned.
// Returns 1 on success, 0 if the slot is already registered.
// KEYS[1] = slot-to-container hash, KEYS[2] = container slots set, KEYS[3] = active containers set.
// ARGV[1] = slot ID string, ARGV[2] = container name.
//
//nolint:gochecknoglobals // Pre-compiled Lua script for atomic slot registration.
var registerSlotScript = redis.NewScript(`
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then
    return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('SADD', KEYS[2], ARGV[1])
redis.call('SADD', KEYS[3], ARGV[2])
return 1
`)

// RegisterSlot atomically assigns a slot to a container.
// Returns ErrSlotAlreadyRunning if the slot is already registered (prevents
// multi-instance TOCTOU between SlotExists and RegisterSlot).
func (s *State) RegisterSlot(ctx context.Context, slotID int, containerName string) error {
	sid := strconv.Itoa(slotID)

	result, err := registerSlotScript.Run(
		ctx, s.rdb,
		[]string{slotToContainerKey, containerSlotsKey(containerName), activeContainersKey},
		sid, containerName,
	).Int()
	if err != nil {
		return errors.Wrap(err, "registering slot atomically")
	}

	if result == 0 {
		return errors.Wrapf(rtferrors.ErrSlotAlreadyRunning, "slot %d", slotID)
	}

	return nil
}

// unregisterSlotScript atomically looks up and removes a slot mapping.
// Returns the container name if found, or empty string if not.
// KEYS[1] = slot-to-container hash.
// ARGV[1] = slot ID string, ARGV[2] = container info key prefix, ARGV[3] = slots suffix.
//
//nolint:gochecknoglobals // Pre-compiled Lua script for atomic slot unregistration.
var unregisterSlotScript = redis.NewScript(`
local containerName = redis.call('HGET', KEYS[1], ARGV[1])
if not containerName then
    return ''
end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('SREM', ARGV[2] .. containerName .. ARGV[3], ARGV[1])
return containerName
`)

// UnregisterSlot atomically removes a slot from its container and returns
// the container name. Returns ErrSlotNotFound when the slot does not exist.
func (s *State) UnregisterSlot(ctx context.Context, slotID int) (string, error) {
	sid := strconv.Itoa(slotID)

	containerName, err := unregisterSlotScript.Run(
		ctx, s.rdb,
		[]string{slotToContainerKey},
		sid, containerInfoKeyPrefix, ":slots",
	).Text()
	if err != nil {
		return "", errors.Wrap(err, "unregistering slot atomically")
	}

	if containerName == "" {
		return "", errors.Wrap(rtferrors.ErrSlotNotFound, "unregistering slot")
	}

	return containerName, nil
}

// GetSlotContainer returns the container name for a given slot.
// Returns ErrSlotNotFound when the slot does not exist, and propagates other
// Redis errors without masking them.
func (s *State) GetSlotContainer(ctx context.Context, slotID int) (string, error) {
	sid := strconv.Itoa(slotID)

	val, err := s.rdb.HGet(ctx, slotToContainerKey, sid).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", errors.Wrap(rtferrors.ErrSlotNotFound, "looking up slot container")
		}

		return "", errors.Wrap(err, "looking up slot container in Redis")
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
//
// NOTE: The Lua script builds keys dynamically from ARGV, which means it
// accesses keys not declared in the KEYS array. This is incompatible with
// Redis Cluster. RedTailFox is designed for single-node Redis only.
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

// slotConfigTTL is the expiration time for stored slot configs.
// Configs are refreshed on each start; expired configs indicate unused slots.
const slotConfigTTL = 7 * 24 * time.Hour

// SaveSlotConfig stores slot configuration in Redis with a TTL for restart capability.
// The TTL prevents unbounded memory growth from orphaned configs.
func (s *State) SaveSlotConfig(ctx context.Context, slotID int, cfg []byte) error {
	key := fmt.Sprintf("%s%d", slotConfigKeyPrefix, slotID)

	if err := s.rdb.Set(ctx, key, cfg, slotConfigTTL).Err(); err != nil {
		return errors.Wrap(err, "saving slot config")
	}

	return nil
}

// GetSlotConfig retrieves stored slot configuration from Redis.
// Returns ErrConfigNotFound when the key does not exist, and propagates
// all other Redis errors without masking them.
func (s *State) GetSlotConfig(ctx context.Context, slotID int) ([]byte, error) {
	key := fmt.Sprintf("%s%d", slotConfigKeyPrefix, slotID)

	val, err := s.rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, errors.Wrap(rtferrors.ErrConfigNotFound, "getting slot config")
		}

		return nil, errors.Wrap(err, "getting slot config from Redis")
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
