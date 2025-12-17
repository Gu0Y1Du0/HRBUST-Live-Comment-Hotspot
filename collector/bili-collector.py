from bilibili_api import live, Credential
from kafka import KafkaProducer
import time
import json
import asyncio
import logging

from kafka.errors import KafkaError

# 初始化日志信息
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("bili-collector")

# 绑定对应的kafka服务
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    retries=3,
    linger_ms=10,
)

# 绑定个人用户账号
credential = Credential(
    sessdata="2ab001ae%2C1781428479%2Ca1f40%2Ac2CjDrF1luymxc3DaCiDjAIYtjEqk1zeQPaxX-SN31BWuLwWaOhOadKDDha0QeZAgAVxQSVm55SlNXOWtQWHprdXBLMEhDYXJNS1J2YjlpTFVXMXZmalp2aDB4SmE2cWVQMXdZYlVlaUZyUllMeWFSQjVyd2UtZVdHVVE2c1ZsZVhfckctZlQ3RUhBIIEC",
    bili_jct="3f448aa896da3f6d52b2b33d7baabcbb",
    buvid3="F7BB05CB-168D-25CE-8E14-5B09DFE6FB9783427infoc",
)

# 房间ID
room_id = 732
# room_id = 923833

# 初始化直播弹幕服务
room = live.LiveDanmaku(room_display_id=room_id, credential=credential)


# 监听弹幕信息
@room.on("DANMU_MSG")
async def on_danmaku(event):
    # 发送者ID
    info = event["data"]["info"]
    user_name = info[2][1]
    content = info[1]
    user_hash = info[0][7]
    # print(user_name, user_hash, msg)

    message = {
        "platform": "bilibili",
        "room_id": room_id,
        "user": {"id": user_hash, "name": user_name},
        "content": content,
        "event_type": "danmaku",
        "ts": int(time.time() * 1000),
    }

    send_to_kafka(room_id=room_id, message=message)


# 发送Json信息到Kafka
def send_to_kafka(room_id, message: dict):
    future = producer.send(
        "danmaku_raw", key=str(room_id).encode("utf-8"), value=message
    )

    def on_success(record_metadata):
        logging.debug(
            f"kafka send success: topic={record_metadata.topic}, "
            f"partition={record_metadata.partition}, "
            f"offset={record_metadata.offset} "
        )

    def on_error(excp: KafkaError):
        logger.error(
            "Kafka send failed",
            exc_info=excp,
        )

    future.add_callback(on_success)
    future.add_errback(on_error)


# 运行
async def main():
    await room.connect()


asyncio.run(main())
