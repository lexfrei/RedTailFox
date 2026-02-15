import json
import time
# from email.header import Header
from loguru import logger

class Head:
    def __init__(self, slots, redis_client, container_name ):
        self.slots = slots
        self.redis = redis_client
        self.last_metrics_versions = {}
        self.CONTAINER_NAME = container_name

    def run(self):
        while True:
            logger.info("Headbear проверяю работу слота")
            state_snapshot = []
            metrics_snapshot = []

            now = int(time.time())

            for slot_id, slot in list(self.slots.items()):
                logger.info("Передаю статус")
                # -------- STATE (всегда) --------
                state_snapshot.append({
                    "slot_id": slot_id,
                    "running": slot.running,
                    "status": slot.status,
                    "last_active": slot.last_active,
                    "ts": now
                })

                # # -------- METRICS (только при изменении) --------
                # v = slot.metrics_version
                # if self.last_metrics_versions.get(slot_id) != v:
                #     self.last_metrics_versions[slot_id] = v
                #     logger.info("Передаю метрики")
                #
                #     metrics_snapshot.append({
                #         "slot_id": slot_id,
                #         "metrics": slot.metrics.copy(),
                #         "ts": now
                #     })

            if state_snapshot:
                self.redis.set(
                    f"hb:container:{self.CONTAINER_NAME}",
                    json.dumps({
                        "container": self.CONTAINER_NAME,
                        "ts": now,
                        "slots": state_snapshot
                    }),
                    ex=3600
                )
                logger.info("Передал статус")


            # if metrics_snapshot:
            #     self.redis.rpush("monitor_metrics", json.dumps({
            #         "container": self.CONTAINER_NAME,
            #         "metrics": metrics_snapshot
            #     }))
            #     logger.info("Передал метрики")

            time.sleep(3)

