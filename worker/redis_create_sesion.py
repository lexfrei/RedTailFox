# Redis клиент
import os

import redis
from dotenv import load_dotenv

load_dotenv()
# Настройки из окружения
REDIS_HOST = os.environ.get("REDIS_HOST", "")
REDIS_PORT = int(os.environ.get("REDIS_PORT", ))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")


redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True  # чтобы получать строки, а не байты
        )