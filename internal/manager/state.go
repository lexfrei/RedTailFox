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

// RegisterSlot assigns a slot to a container in Redis.
func (s *State) RegisterSlot(ctx context.Context, slotID int, containerName string) error {
	sid := strconv.Itoa(slotID)

	err := s.rdb.HSet(ctx, slotToContainerKey, sid, containerName).Err()
	if err != nil {
		return errors.Wrap(err, "setting slot-to-container mapping")
	}

	err = s.rdb.SAdd(ctx, containerSlotsKey(containerName), sid).Err()
	if err != nil {
		return errors.Wrap(err, "adding slot to container set")
	}

	err = s.rdb.SAdd(ctx, activeContainersKey, containerName).Err()
	if err != nil {
		return errors.Wrap(err, "adding container to active set")
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

	s.rdb.HDel(ctx, slotToContainerKey, sid)
	s.rdb.SRem(ctx, containerSlotsKey(containerName), sid)

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

// PickContainer finds a container with available slot capacity.
// Returns empty string if all containers are full.
func (s *State) PickContainer(ctx context.Context) (string, error) {
	active, err := s.rdb.SMembers(ctx, activeContainersKey).Result()
	if err != nil {
		return "", errors.Wrap(err, "listing active containers")
	}

	for _, name := range active {
		count, err := s.rdb.SCard(ctx, containerSlotsKey(name)).Result()
		if err != nil {
			continue
		}

		if int(count) < s.maxSlots {
			return name, nil
		}
	}

	return "", nil
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

// RemoveContainer removes a container from the active set and deletes its slot set.
func (s *State) RemoveContainer(ctx context.Context, containerName string) error {
	err := s.rdb.Del(ctx, containerSlotsKey(containerName)).Err()
	if err != nil {
		return errors.Wrap(err, "deleting container slot set")
	}

	err = s.rdb.SRem(ctx, activeContainersKey, containerName).Err()
	if err != nil {
		return errors.Wrap(err, "removing container from active set")
	}

	return nil
}

// SlotExists checks if a slot is currently registered.
func (s *State) SlotExists(ctx context.Context, slotID int) bool {
	sid := strconv.Itoa(slotID)

	return s.rdb.HExists(ctx, slotToContainerKey, sid).Val()
}

func containerSlotsKey(containerName string) string {
	return containerInfoKeyPrefix + containerName + ":slots"
}
