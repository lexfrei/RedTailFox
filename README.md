# RedTailFox

Single-node container orchestrator with Redis-based coordination. Manages isolated worker processes across containers with automatic scaling, health monitoring, and self-healing.

## Architecture

RedTailFox is a single Go binary with three subcommands:

- **manager** - Distributes tasks across containers, manages slot assignments, handles scaling.
- **worker** - Runs inside each container, manages slot lifecycle, publishes heartbeats.
- **monitor** - Watches heartbeat keys in Redis, detects failures, triggers restarts.

All components communicate through Redis queues and keys.

## Build

```text
go build -o redtailfox ./cmd/redtailfox
```

Container image:

```text
podman build --tag redtailfox:latest --file Containerfile .
```

## Run

```text
# Start the manager (requires container runtime access)
./redtailfox manager

# Start a worker (typically launched by the manager inside a container)
./redtailfox worker

# Start the monitor
./redtailfox monitor
```

Or use compose:

```text
podman compose up
```

## Configuration

All settings are read from environment variables.

### Redis

| Variable | Default | Description |
| --- | --- | --- |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | (empty) | Redis password |

### Manager

| Variable | Default | Description |
| --- | --- | --- |
| `MAX_SLOTS_PER_CONTAINER` | `10` | Maximum slots per container |
| `WORKER_IMAGE` | `fox_worker:latest` | Container image for workers |
| `WORKER_CONTAINER_PREFIX` | `fox_worker` | Container name prefix |
| `COMMAND_CHANNEL_PREFIX` | `COMMAND_CHANNEL` | Redis queue prefix for commands |
| `WORKER_TASKS_LIST` | `manager_tasks` | Redis queue for incoming tasks |
| `WORKER_REPORTS_CHANNEL` | `worker_reports` | Redis queue for worker reports |
| `DB_WRITE_QUEUE` | `db_write_requests` | Redis queue for database write events |

### Worker

| Variable | Default | Description |
| --- | --- | --- |
| `COMMAND_CHANNEL` | `worker_commands` | Redis queue for receiving commands |
| `EVENT_CHANNEL` | `events` | Redis queue for events |
| `CONTAINER_NAME` | `unknown_container` | Name of this container |

### Monitor

| Variable | Default | Description |
| --- | --- | --- |
| `CHECK_INTERVAL` | `10` | Health check interval in seconds |
| `MAX_SILENCE_SECONDS` | `500` | Maximum heartbeat age before considering container stale |
| `SLOT_IDLE_TIMEOUT` | `600` | Maximum slot inactivity before restart |

## How It Works

1. A task is pushed to the manager queue in Redis.
2. The manager assigns the task to a slot in an existing container or starts a new one.
3. The worker inside the container receives the command and starts the slot.
4. Each worker publishes heartbeats to Redis every 3 seconds.
5. The monitor scans heartbeat keys and detects stale containers or idle slots.
6. On failure detection, the monitor pushes restart commands back to the manager queue.

## License

MIT
