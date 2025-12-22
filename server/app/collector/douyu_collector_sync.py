from pydouyu_async.async_client import AsyncClient
from kafka import KafkaProducer
from kafka.errors import KafkaError
from app.core.database import pool
from dotenv import load_dotenv
import os
import sys
import time
import json
import logging
import asyncio
import redis
import argparse

# 弹幕CONFIG
# ROOM_IDS = {
#     '玩机器': 6979222,
#     '曹永富': 34972,
#     '电棍': 12306,
#     '张顺飞': 2561707,
# }
current_dir = os.path.dirname(os.path.dirname(__file__))
env_path = os.path.join(current_dir, "../.env")
load_dotenv(env_path)
BOOTSTRAP_SERVERS = [os.getenv("KAFKA_BOOTSTRAP_SERVERS", "Master:9092")]

# 初始化日志信息
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("douyu-collector")

# # 弹幕抓取
# async def chatmsg_handler(msg):
#     danmaku = {
#         "platform": "douyu",
#         "room_id": room_id,
#         "user": {
#             "id": msg["uid"],
#             "name": msg["nn"],
#         },
#         "content": msg["txt"],
#         "event_type": "danmaku",
#         "ts": int(time.time() * 1000),
#     }
#     print(danmaku)
#     sys.stdout.flush()

#     send_to_kafka(room_id=room_id, message=danmaku)

# 弹幕上传
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    retries=3,      # 失败重试次数
    linger_ms=10,
)

r = redis.Redis(connection_pool=pool)

def get_args():
    parser = argparse.ArgumentParser(description="Bilibili Live Collector")
    # 定义需要接受的参数
    parser.add_argument(
        "--room-id", type=int, required=True, help="直播间ID(长号短号都可以)"
    )

    return parser.parse_args()

# 获取参数
try:
    args = get_args()
    room_id = args.room_id
    logger.info(f"正在启动直播间监控，目标房间: {room_id}")
except Exception as e:
    logger.error(f"参数解析失败: {e}")
    sys.exit(1)

unique_room_id = f"douyu:{room_id}"

# 弹幕抓取
async def chatmsg_handler(msg):
    danmaku = {
        "platform": "douyu",
        "room_id": room_id,
        "user": {
            "id": msg["uid"],
            "name": msg["nn"],
        },
        "content": msg["txt"],
        "event_type": "danmaku",
        "ts": int(time.time() * 1000),
    }
    # print(danmaku)
    # sys.stdout.flush()

    send_to_kafka(room_id=room_id, message=danmaku)

def send_to_kafka(room_id, message: dict):
    future = producer.send(
        topic = "danmaku_raw",
        key = str(room_id).encode('utf-8'),
        value = message
    )

    def on_success(record_metadata):
        logging.debug(
            f"kafka send success: topic={record_metadata.topic}, "
            f"partition={record_metadata.partition}, "
            f"offset={record_metadata.offset} "
        )
    def on_error(excp):
        logging.error(
            f"kafka send failed",
            exc_info=excp,
        )
    future.add_callback(on_success)
    future.add_errback(on_error)

async def main():
    # client = AsyncClient(room_id=room_id)
    # client.add_handler('chatmsg', chatmsg_handler)
    # await client.start()
    try:
        client = AsyncClient(room_id=room_id)
        client.add_handler('chatmsg', chatmsg_handler)
        await client.start()
        logger.info(f"更新Redis状态: {unique_room_id} -> RUNNING")
        r.hset("monitor:task_status", unique_room_id, "RUNNING")
    except Exception as e:
        logger.error(f"直播连接中断: {e}")
    finally:
        logger.info(f"监控停止，更新Redis状态: {unique_room_id} -> STOPPED")
        r.hset("monitor:task_status", unique_room_id, "STOPPED")
        r.close()

if __name__ == "__main__":
    # asyncio.run(main())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

