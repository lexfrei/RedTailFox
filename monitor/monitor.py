import time
import json
from loguru import logger
from redis_sesion import create_sesion_redit

r = create_sesion_redit()
ACTIVE_CONTAINERS_KEY = "manager:active_containers"
WORKER_TASKS_LIST = "autoreply_queue"
HB_KEY_PREFIX = "hb:container:"
CHECK_INTERVAL = 10
MAX_SILENCE_SECONDS = 500
SLOT_IDLE_TIMEOUT = 600

# in-memory защита от ложных рестартов
container_failures = {}


def iter_hb_keys(redis):
    cursor = 0
    while True:
        cursor, keys = redis.scan(
            cursor=cursor,
            match=f"{HB_KEY_PREFIX}*",
            count=100
        )
        for k in keys:
            yield k
        if cursor == 0:
            break


def monitor_step():
    now = time.time()
    # Сет для хранения имен контейнеров, которые прислали хоть какой-то HB
    seen_containers = set()

    # --- ШАГ 1: Проверяем существующие Heartbeat ключи ---
    for key in iter_hb_keys(r):
        raw = r.get(key)
        if not raw:
            continue

        try:
            data = json.loads(raw)
            container = data["container"]
            # Добавляем в список тех, у кого есть ключ в Redis
            seen_containers.add(container)

            slots = data.get("slots", [])
            last_ts = data.get("ts", 0)

            # Проверка зависания (время в ключе устарело)
            if now - last_ts > MAX_SILENCE_SECONDS:
                cnt = container_failures.get(container, 0) + 1
                container_failures[container] = cnt
                if cnt >= 2:
                    logger.error(f"🚨 Контейнер {container} завис (старый ts). Рестарт.")
                    trigger_container_restart(container)
                    container_failures.pop(container, None)
                else:
                    logger.warning(f"⚠️ Контейнер {container} подозрительный ({cnt}/2)")
                # Пропускаем проверку слотов, если весь контейнер висит
                continue
            else:
                container_failures.pop(container, None)

            # Проверка каждого слота
            for slot in slots:
                slot_id = slot["slot_id"]
                if not slot.get("running", True):
                    trigger_slot_restart(container, slot_id)
                    continue

                last_active = slot.get("last_active", now)
                if now - last_active > SLOT_IDLE_TIMEOUT:
                    logger.error(f"⏳ Слот {slot_id} в {container} замерз")
                    trigger_slot_restart(container, slot_id)

        except Exception as e:
            logger.error(f"Ошибка парсинга HB {key}: {e}")

    # --- ШАГ 2: Проверяем контейнеры, которых ВООБЩЕ нет в Redis ---
    try:
        # Получаем список всех контейнеров, которые ДОЛЖНЫ работать
        active_names_in_redis = r.smembers(ACTIVE_CONTAINERS_KEY)
        for name in active_names_in_redis:
            name_str = name.decode() if isinstance(name, bytes) else name

            # Если контейнера нет в тех, кого мы видели на Шаге 1
            if name_str not in seen_containers:
                logger.critical(f"🚨 Контейнер {name_str} ВООБЩЕ не прислал Heartbeat! Рестарт.")
                trigger_container_restart(name_str)

    except Exception as e:
        logger.error(f"Ошибка при проверке отсутствующих HB: {e}")


def trigger_slot_restart(container, slot_id):
    """Отправляет команду на перезапуск слота в очередь менеджера"""
    # Менеджеру нужна команда 'start', чтобы запустить слот заново
    # Если слот завис, менеджер сначала должен его 'стопнуть' (очистить Redis)
    r.lpush(
        WORKER_TASKS_LIST, # Очередь, которую слушает менеджер
        json.dumps({
            "command": "restart_slot", # Новая команда для менеджера
            "slot_id": slot_id,
            "container_name": container,
            "initiated_by": "monitor"
        })
    )
    logger.info(f"🚀 Monitor: Sent restart_slot for {slot_id}")

def trigger_container_restart(container):
    """Отправляет команду на полный перезапуск контейнера"""
    r.lpush(
        WORKER_TASKS_LIST,
        json.dumps({
            "command": "restart_container", # Новая команда для менеджера
            "container_name": container,
            "initiated_by": "monitor"
        })
    )
    logger.info(f"🚀 Monitor: Sent restart_container for {container}")


if __name__ == "__main__":
    while True:
        monitor_step()
        time.sleep(CHECK_INTERVAL)
