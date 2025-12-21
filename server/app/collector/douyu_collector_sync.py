from pydouyu_async.async_client import AsyncClient
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import time
import json
import logging
import asyncio

# 弹幕CONFIG
ROOM_IDS = {
    '玩机器': 6979222,
    '曹永富': 34972,
    '电棍': 12306
}
room_id = ROOM_IDS['玩机器']

# 初始化日志信息
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("douyu-collector")

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
    print(danmaku)
    sys.stdout.flush()

    send_to_kafka(room_id=room_id, message=danmaku)

# 队列CONFIG
KAFKA_BROKERS=["localhost:9092"]
TOPIC_NAME= "danmaku_row"
# 弹幕上传
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    retries=3,      # 失败重试次数
    linger_ms=10,
)

def send_to_kafka(room_id, message: dict):
    future = producer.send(
        topic = TOPIC_NAME,
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
    client = AsyncClient(room_id=room_id)
    client.add_handler('chatmsg', chatmsg_handler)
    await client.start()

if __name__ == "__main__":
    asyncio.run(main())

