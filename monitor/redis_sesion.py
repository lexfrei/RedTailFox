import os
import asyncio
import redis.asyncio as redis
import redis
import time
from loguru import logger
from dotenv import load_dotenv


def create_sesion_redit():
    load_dotenv()
    try:
        r = redis.Redis(
            host=os.environ.get("REDIS_HOST"),
            port=os.environ.get("REDIS_PORT"),
            password=os.environ.get("REDIS_PASSWORD"),
            decode_responses=True,
            socket_connect_timeout=10
        )
        logger.success(f"Удачно подключились к Reddis {r}")
        return r
    except Exception as ersa:
        logger.error(f"Ошибка при подключения Reddis : {ersa} ")


if __name__ == "__main__":
    create_sesion_redit()