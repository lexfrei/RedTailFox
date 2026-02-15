import os
import json
import time
import redis
import threading
import signal
import sys
from loguru import logger
from dotenv import load_dotenv

# Импорты ваших модулей
from metrica_class import MetricContext
from heartbeat import Head
from type_status import SlotStatus
from report import report
from redis_create_sesion import redis_client


load_dotenv()

# Настройки из окружения
COMMAND_CHANNEL = os.environ.get("COMMAND_CHANNEL", "worker_commands")  #  Канал приема команд от менеджера
EVENT_CHANNEL = os.environ.get("EVENT_CHANNEL", "events")
CONTAINER_NAME = os.environ.get("CONTAINER_NAME", "unknown_container")

slots = {}


class Slot:
    def __init__(self, slot_id, config):
        self.metrics_ctx = MetricContext(self)
        self.slot_id = slot_id
        self.config = config
        self.running = False
        self.thread = None
        self.last_check_time = 0
        self.status = SlotStatus.IDLE.value
        self.last_active = time.time()
        self.metrics_version = 0

    def _set_status(self, status: SlotStatus | str):
        self.status = status.value if isinstance(status, SlotStatus) else status
        self.last_active = time.time()

    def start(self):
        """Запуск основного процесса слота"""
        try:
            logger.info(f"[{self.slot_id}] Попытка запуска в {CONTAINER_NAME}...")





            self.running = True
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()

            # 2. ОТЧЕТ МЕНЕДЖЕРУ (Исправлено имя аргумента)
            report(
                slot_id=self.slot_id,
                status="started",
                conteiner_name=CONTAINER_NAME  # Было conteiner_name
            )
            logger.success(f"[{self.slot_id}] Слот успешно запущен")

        except Exception as e:
            logger.error(f"[{self.slot_id}] Ошибка старта: {e}")
            report(
                slot_id=self.slot_id,
                status="error",
                error_text=str(e),
                conteiner_name=CONTAINER_NAME
            )
            self.running = False

    def run(self):
        """Цикл работы слота"""
        # Берем интервал из конфига или ставим 15 минут
        check_interval = self.config.get("bot", {}).get("check_interval", 900)   # Значения интервала между работой
        logger.info("Живой ожидаю ")
        while self.running:
            try:
                self._set_status(status=SlotStatus.IDLE)
                now = time.time()
                if now - self.last_check_time >= check_interval:
                    logger.info(f"[{self.slot_id}] Simulated task execution")

                    self._set_status(SlotStatus.READ_PENDING_CHAT)
                    logger.success(f"Work to {self.slot_id}. Processing job...")

                    self._set_status(SlotStatus.DIRECT_CHECK)
                    logger.success(f"Work to {self.slot_id}. Processing job...")



                    self.last_check_time = time.time()
                    self._set_status(SlotStatus.IDLE)
                    logger.info("Job complete")

                time.sleep(5)  # Пауза между проверками времени
            except Exception as e:
                logger.error(f"[{self.slot_id}] Критическая ошибка в цикле: {e}")
                time.sleep(30)  # Пауза при ошибке, чтобы не спамить в лог

    def stop(self):
        """Мягкая остановка слота"""
        if not self.running:
            return

        logger.info(f"[{self.slot_id}] Остановка...")
        self.running = False

        # Даем время на выход из цикла run
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)

        report(
            slot_id=self.slot_id,
            status="stopped",
            conteiner_name=CONTAINER_NAME
        )


def handle_command(message):
    """Обработка команд из Redis"""
    try:
        data = json.loads(message["data"])
        slot_id = str(data.get("slot_id"))
        command = data.get("command")
        config = data.get("config")

        if command == "start":
            if slot_id in slots and slots[slot_id].running:
                logger.warning(f"[{slot_id}] Слот уже активен.")
                return

            new_slot = Slot(slot_id, config)
            slots[slot_id] = new_slot
            new_slot.start()

        elif command == "stop":
            if slot_id in slots:
                slots[slot_id].stop()
                del slots[slot_id]
                logger.info(f"[{slot_id}] Слот удален из памяти воркера.")
            else:
                # Даже если слота нет, отвечаем менеджеру, что он "стопнут"
                report(slot_id=slot_id, status="stopped", conteiner_name=CONTAINER_NAME)

    except Exception as e:
        logger.error(f"Ошибка парсинга команды: {e}")


def command_listener():
    """Слушатель очереди команд"""
    logger.info(f"📥 Слушаем команды: {COMMAND_CHANNEL}")
    while True:
        try:
            # Используем блокирующий RPOP (BRPOP)
            item = redis_client.brpop(COMMAND_CHANNEL, timeout=10)
            if item:
                _, message = item
                handle_command({"data": message})
        except Exception as e:
            logger.error(f"Ошибка listener: {e}")
            time.sleep(2)


def shutdown_handler(signum, frame):
    """Корректное завершение при остановке контейнера"""
    logger.warning(f"⛔ Получен сигнал {signum}. Завершаем работу слотов...")
    for slot_id in list(slots.keys()):
        slots[slot_id].stop()

    # Небольшая пауза, чтобы Redis успел обработать отчеты report()
    time.sleep(1)
    sys.exit(0)


def main():
    # Регистрация сигналов Docker (SIGTERM)
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Поток команд
    cmd_thread = threading.Thread(target=command_listener, daemon=True)
    cmd_thread.start()

    # Поток Heartbeat (из вашего модуля Head)
    # head = Head(slots, redis_client, CONTAINER_NAME)
    # hb_thread = threading.Thread(target=head.run, daemon=True)
    # hb_thread.start()

    logger.success(f"🚀 Воркер {CONTAINER_NAME} готов к работе")

    # Основной поток просто спит, пока работают фоновые треды
    while True:
        time.sleep(10)


if __name__ == "__main__":
    main()