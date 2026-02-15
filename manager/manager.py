import os
import json
import time
import threading
from loguru import logger
from redis_sesion import create_sesion_redit

from RedTailFox.docker.docker_utils import (
    start_new_container, stop_container_by_name, list_managed_containers,
    MAX_SLOTS_PER_CONTAINER, CONTAINER_NAME_PREFIX
)

# --- Настройки Redis (Ключи) ---
SLOT_TO_CONTAINER_KEY = "manager:slot_to_container"  # hash: slot_id -> container_name
CONTAINER_INFO_KEY_PREFIX = "manager:container:"  # prefix для сетов слотов
ACTIVE_CONTAINERS_KEY = "manager:active_containers"  # set: список имен живых контейнеров
CONTAINER_COUNTER_KEY = "manager:container_counter"  # int: счетчик для имен
SLOT_CONFIG_KEY_PREFIX = "manager:config:slot:"  # string: хранение конфига для рестартов

# --- Настройки окружения ---
WORKER_TASKS_LIST = os.getenv("WORKER_TASKS_LIST", "manager_tasks")
WORKER_REPORTS_CHANNEL = os.getenv("WORKER_REPORTS_CHANNEL", "worker_reports")
DB_WRITE_QUEUE = os.getenv("DB_WRITE_QUEUE", "db_write_requests")
COMMAND_CHANNEL_PREFIX = os.getenv("COMMAND_CHANNEL_PREFIX", "COMMAND_CHANNEL")

r = create_sesion_redit()

# Локальный кэш для быстрой фильтрации
_local_containers_cache = {}


# --- Вспомогательные функции ---

def _container_slots_key(container_name: str) -> str:
    return f"{CONTAINER_INFO_KEY_PREFIX}{container_name}:slots"


def _ensure_container_counter():
    if r.get(CONTAINER_COUNTER_KEY) is None:
        r.set(CONTAINER_COUNTER_KEY, 0)


def get_queue_for_container(container_name: str) -> str:
    try:
        idx = container_name.split("_")[-1]
        return f"{COMMAND_CHANNEL_PREFIX}_{idx}"
    except Exception as e:
        logger.error(f"Ошибка определения очереди для {container_name}: {e}")
        return f"{COMMAND_CHANNEL_PREFIX}_unknown"


# --- Работа с конфигурациями (Для Авто-рестартов) ---

def save_slot_config(slot_id: int, config: dict):
    """Сохраняет конфиг в Redis, чтобы Monitor мог перезапустить слот без внешних данных."""
    r.set(f"{SLOT_CONFIG_KEY_PREFIX}{slot_id}", json.dumps(config))


def get_slot_config(slot_id: int) -> dict:
    """Извлекает сохраненный конфиг из Redis."""
    raw = r.get(f"{SLOT_CONFIG_KEY_PREFIX}{slot_id}")
    return json.loads(raw) if raw else None


# --- Логика управления состоянием ---

def get_containers_state_from_redis():
    global _local_containers_cache
    _local_containers_cache = {}
    active_names = r.smembers(ACTIVE_CONTAINERS_KEY)

    for name in active_names:
        name_str = name if isinstance(name, str) else name.decode()
        slots = r.smembers(_container_slots_key(name_str))
        _local_containers_cache[name_str] = set(int(x) for x in slots) if slots else set()


def pick_container_for_start():
    get_containers_state_from_redis()
    for container_name, slots in _local_containers_cache.items():
        if len(slots) < MAX_SLOTS_PER_CONTAINER:
            return container_name
    return None


def register_slot_in_container(slot_id: int, container_name: str):
    r.hset(SLOT_TO_CONTAINER_KEY, slot_id, container_name)
    r.sadd(_container_slots_key(container_name), slot_id)
    r.sadd(ACTIVE_CONTAINERS_KEY, container_name)


def unregister_slot(slot_id: int):
    container = r.hget(SLOT_TO_CONTAINER_KEY, slot_id)
    if container:
        container_str = container if isinstance(container, str) else container.decode()
        r.hdel(SLOT_TO_CONTAINER_KEY, slot_id)
        r.srem(_container_slots_key(container_str), slot_id)
        return container_str
    return None


def publish_db_write(slot_id: int, status: str, initiated_by: str, error_text: str = None):
    msg = {
        "slot_id": int(slot_id),
        "status": status,
        "initiated_by": initiated_by,
        "timestamp": int(time.time())
    }
    if error_text: msg["error_text"] = error_text
    r.lpush(DB_WRITE_QUEUE, json.dumps(msg))


# --- Основные действия ---

def handle_start_task(task: dict):
    slot_id = int(task["slot_id"])
    config = task.get("config")

    # Если конфига нет в задаче (рестарт от монитора), берем из Redis
    if not config:
        config = get_slot_config(slot_id)
        if not config:
            logger.error(f"❌ Рестарт невозможен: конфиг для слота {slot_id} отсутствует в Redis")
            return
    else:
        # Сохраняем/обновляем конфиг при каждом "чистом" старте
        save_slot_config(slot_id, config)

    if r.hexists(SLOT_TO_CONTAINER_KEY, slot_id):
        logger.warning(f"Слот {slot_id} уже запущен. Пропуск.")
        return

    container = pick_container_for_start()
    if not container:
        try:
            _ensure_container_counter()
            idx = r.incr(CONTAINER_COUNTER_KEY)
            container = f"{CONTAINER_NAME_PREFIX}_{idx}"
            start_new_container(idx)
            r.sadd(ACTIVE_CONTAINERS_KEY, container)
        except Exception as e:
            publish_db_write(slot_id, "error", "manager", error_text=f"docker_fail: {e}")
            return

    register_slot_in_container(slot_id, container)
    queue = get_queue_for_container(container)
    message = {"slot_id": slot_id, "command": "start", "config": config}

    try:
        r.lpush(queue, json.dumps(message))
        logger.success(f"Команда 'start' для {slot_id} отправлена в {container}")
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        unregister_slot(slot_id)


def handle_stop_task(task: dict):
    slot_id = int(task["slot_id"])
    container = r.hget(SLOT_TO_CONTAINER_KEY, slot_id)

    if not container:
        publish_db_write(slot_id, "stop", "manager")
        return

    container_str = container if isinstance(container, str) else container.decode()
    queue = get_queue_for_container(container_str)
    try:
        r.lpush(queue, json.dumps({"slot_id": slot_id, "command": "stop"}))
        logger.info(f"Запрос 'stop' для {slot_id} отправлен")
    except Exception as e:
        logger.error(f"Ошибка стопа: {e}")


def handle_restart_slot(task: dict):
    slot_id = int(task["slot_id"])
    logger.warning(f"🔄 Рестарт слота {slot_id} по сигналу Monitor")

    unregister_slot(slot_id)
    publish_db_write(slot_id, "restarting", "monitor", error_text="heartbeat_timeout")

    # Запускаем заново (config подтянется из Redis внутри handle_start_task)
    handle_start_task({"slot_id": slot_id})


def handle_restart_container(task: dict):
    container_name = task.get("container_name")
    logger.error(f"🚨 Полный рестарт зависшего контейнера {container_name}")

    slots = r.smembers(_container_slots_key(container_name))
    stop_container_by_name(container_name)

    for s in slots:
        s_id = int(s)
        unregister_slot(s_id)
        publish_db_write(s_id, "restarting", "monitor", error_text="container_freeze")
        handle_start_task({"slot_id": s_id})

    r.delete(_container_slots_key(container_name))
    r.srem(ACTIVE_CONTAINERS_KEY, container_name)


def handle_worker_report(data: dict):
    try:
        slot_id = str(data.get("slot_id"))
        status = data.get("status")
        container_name = data.get("container_name")

        if status in ("stopped", "error"):
            target_container = unregister_slot(int(slot_id))
            publish_db_write(int(slot_id), status, "worker", error_text=data.get("error_text"))

            if target_container:
                if r.scard(_container_slots_key(target_container)) == 0:
                    logger.info(f"Контейнер {target_container} пуст. Остановка.")
                    stop_container_by_name(target_container)
                    r.delete(_container_slots_key(target_container))
                    r.srem(ACTIVE_CONTAINERS_KEY, target_container)

        elif status == "started":
            publish_db_write(int(slot_id), "run_worker", "manager")

    except Exception as e:
        logger.error(f"Ошибка отчета: {e}")


# --- Циклы ---

def sync_docker_loop():
    while True:
        try:
            actual = list_managed_containers()
            redis_containers = [c.decode() if isinstance(c, bytes) else c for c in r.smembers(ACTIVE_CONTAINERS_KEY)]

            for rc in redis_containers:
                if rc not in actual:
                    logger.warning(f"Контейнер {rc} исчез из Docker. Очистка.")
                    zombie_slots = r.smembers(_container_slots_key(rc))
                    for s in zombie_slots:
                        s_id = int(s)
                        r.hdel(SLOT_TO_CONTAINER_KEY, s_id)
                        publish_db_write(s_id, "error", "system", error_text="container_vanished")
                    r.delete(_container_slots_key(rc))
                    r.srem(ACTIVE_CONTAINERS_KEY, rc)
        except Exception as e:
            logger.error(f"Sync error: {e}")
        time.sleep(60)


def tasks_loop():
    while True:
        try:
            item = r.brpop(WORKER_TASKS_LIST, timeout=5)
            if not item: continue
            task = json.loads(item[1])
            cmd = task.get("command") or task.get("action")

            if cmd in ("start", "run", "start_worker"):
                handle_start_task(task)
            elif cmd == "stop":
                handle_stop_task(task)
            elif cmd == "restart_slot":
                handle_restart_slot(task)
            elif cmd == "restart_container":
                handle_restart_container(task)
        except Exception as e:
            logger.error(f"Tasks error: {e}")


def reports_listener():
    while True:
        try:
            item = r.brpop(WORKER_REPORTS_CHANNEL, timeout=5)
            if not item: continue
            handle_worker_report(json.loads(item[1]))
        except Exception as e:
            logger.error(f"Reports error: {e}")


def main():
    logger.info("=== Менеджер запущен ===")
    get_containers_state_from_redis()
    threading.Thread(target=tasks_loop, daemon=True).start()
    threading.Thread(target=reports_listener, daemon=True).start()
    threading.Thread(target=sync_docker_loop, daemon=True).start()
    while True: time.sleep(1)


if __name__ == "__main__":
    main()