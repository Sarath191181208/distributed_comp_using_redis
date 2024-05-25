import random
from datetime import datetime
from json import dumps
from time import sleep

from uuid import uuid4

import redis
import config


def redis_db():
    db = redis.Redis(
        host=config.redis_host,
        port=config.redis_port,
        db=config.redis_db_number,
        password=config.redis_password,
        decode_responses=True,
    )

    db.ping()
    return db


def redis_queue_push(db: redis.Redis, msg):
    db.lpush(config.redis_queue_name, msg)


def main(n: int, delay: float = 1):
    db = redis_db()

    for i in range(n):
        msg = {
            "id": str(uuid4()),
            "ts": datetime.now(),
            "data": {
                "message_number": i,
                "x": random.randrange(0, 100),
                "y": random.randrange(0, 100),
            },
        }

        msg_json = dumps(msg, default=str)

        print(f"Pushing message {i}: {msg_json}")
        redis_queue_push(db, msg_json)

        sleep(delay)


if __name__ == "__main__":
    main(300, 0.1)
