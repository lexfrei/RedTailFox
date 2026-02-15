import os
import time
import docker
from loguru import logger

DOCKER_IMAGE = os.getenv("WORKER_IMAGE", "fox_worker:latest")
CONTAINER_NAME_PREFIX = os.getenv("WORKER_CONTAINER_PREFIX", "fox_worker")
COMMAND_CHANNEL_PREFIX = os.getenv("COMMAND_CHANNEL_PREFIX", "COMMAND_CHANNEL")
MAX_SLOTS_PER_CONTAINER = int(os.getenv("MAX_SLOTS_PER_CONTAINER", "10"))

# Redis настройки
REDIS_HOST = os.environ.get("REDIS_HOST", "")   #Добавление переменых в новый контейнер
REDIS_PORT = os.environ.get("REDIS_PORT", "")   #Добавление переменых в новый контейнер
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")    #Добавление переменых в контейнер
EVENT_CHANNEL = os.environ.get("EVENT_CHANNEL", "events")

try:
    client = docker.from_env()
except Exception as e:
    logger.error(f"Не удалось подключиться к Docker Daemon: {e}")
    # В некоторых средах может потребоваться инициализация позже или специфичный URL
    client = None


def start_new_container(index: int, env_vars: dict = None):
    name = f"{CONTAINER_NAME_PREFIX}_{index}"
    channel = f"{COMMAND_CHANNEL_PREFIX}_{index}"

    env = {
        "REDIS_HOST": REDIS_HOST,   #Добавление переменых новый в контейнер
        "REDIS_PORT": REDIS_PORT,   #Добавление переменых новый в контейнер
        "REDIS_PASSWORD": REDIS_PASSWORD,  #Добавление переменых в новый контейнер
        "COMMAND_CHANNEL": channel,
        "EVENT_CHANNEL": EVENT_CHANNEL,
        "CONTAINER_INDEX": str(index),
        "CONTAINER_NAME": str(name)
    }

    if env_vars:
        env.update(env_vars)

    logger.info(f"Запускаем контейнер {name} (Image: {DOCKER_IMAGE})")

    try:
        container = client.containers.run(
            DOCKER_IMAGE,
            name=name,
            environment=env,
            detach=True,
            restart_policy={"Name": "unless-stopped"},
        )
        # Уменьшил sleep до 2 сек, так как менеджер и так ждет отчета от воркера
        time.sleep(2.0)
        logger.success(f"Контейнер {name} запущен (id={container.short_id})")
        return container, channel
    except docker.errors.APIError as e:
        logger.error(f"Ошибка Docker API при запуске {name}: {e}")
        raise


def stop_container_by_name(name: str):
    try:
        c = client.containers.get(name)
        logger.info(f"Останавливаем и удаляем контейнер {name}")
        c.stop(timeout=10)  # Даем воркерам время на Graceful Shutdown
        c.remove()
        logger.success(f"Контейнер {name} полностью удален")
        return True
    except docker.errors.NotFound:
        return False
    except Exception as e:
        logger.error(f"Ошибка при удалении {name}: {e}")
        return False


def list_managed_containers():
    """
    Возвращает список ИМЕН (строк) только работающих контейнеров.
    Это критично для корректной работы sync_docker_loop в manager.py.
    """
    if not client:
        return []

    try:
        # Фильтруем только запущенные (status=running)
        containers = client.containers.list(filters={"name": CONTAINER_NAME_PREFIX, "status": "running"})
        # Возвращаем именно имена (строки)
        return [c.name for c in containers]
    except Exception as e:
        logger.error(f"Ошибка при получении списка контейнеров: {e}")
        return []