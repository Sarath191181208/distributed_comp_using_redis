import random 
from json import loads 

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

def redis_queue_pop(db: redis.Redis) -> str:
    _, msg_json = db.brpop(config.redis_queue_name)
    return msg_json

def process_msg(db, msg_json):
    msg = loads(msg_json)
    print(f"Processing message {msg['id']} with data {msg['data']}")

    procss_ok = random.choices([True, False], weights=[0.9, 0.1], k=1)[0]
    if procss_ok:
        print("Processing OK")
    else:
        print("Processing failed")
        redis_queue_push(db, msg_json)

def main():
    db = redis_db()

    while True:
        msg_json = redis_queue_pop(db)
        process_msg(db, msg_json)

if __name__ == "__main__":
    main()
