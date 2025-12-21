import time
import json
import asyncio
import logging
import argparse
import sys
import os
import redis
from bilibili_api import live, Credential
from kafka import KafkaProducer
from kafka.errors import KafkaError
from app.core.database import pool
from dotenv import load_dotenv

# --- 配置 ---
current_dir = os.path.dirname(os.path.dirname(__file__))
env_path = os.path.join(current_dir, "../.env")
load_dotenv(env_path)
BOOTSTRAP_SERVERS = [os.getenv("KAFKA_BOOTSTRAP_SERVERS", "hadoop01:9092")]

# 初始化日志信息
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("bili-collector")

# 绑定对应的kafka服务
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    retries=3,
    linger_ms=10,
)

# 绑定个人用户账号
credential = Credential(
    sessdata="00435fbb%2C1781689028%2C3b071%2Ac2CjCxxglx2ul51NCu_j3HO-ArpqZrsjllKsN0G_v0So9IOD1KKk8hbJiKxVOq0UoZIkwSVm05VnBZYUtTSUNBb2FLbVBhRWFtMjVTaVZfNW8yNFFBYmZGakx3UW9WQUVta2NoSkVBaHE1QUFoNW5VdjNIZ2lOcUk2dE92SXp4RmM1N2VWU1g3WmlnIIEC",
    bili_jct="24dbe082472ec40ed59f216c6d13738a",
    buvid3="F7BB05CB-168D-25CE-8E14-5B09DFE6FB9783427infoc",
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

# 房间ID
# room_id = 732
# room_id = 923833

# 初始化直播弹幕服务
room = live.LiveDanmaku(room_display_id=room_id, credential=credential)

unique_room_id = f"bilibili:{room_id}"


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
        "room_id": unique_room_id,
        "user": {"id": user_hash, "name": user_name},
        "content": content,
        "event_type": "danmaku",
        "ts": int(time.time() * 1000),
    }

    send_to_kafka(room_id=unique_room_id, message=message)


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
    try:
        await room.connect()
        logger.info(f"更新Redis状态: {unique_room_id} -> RUNNING")
        r.hset("monitor:task_status", unique_room_id, "RUNNING")

        await room.connect()
    except Exception as e:
        logger.error(f"直播连接中断: {e}")
    finally:
        # 结束/报错/退出时标记
        logger.info(f"监控停止，更新Redis状态: {unique_room_id} -> STOPPED")
        r.hset("monitor:task_status", unique_room_id, "STOPPED")
        r.close()  # 归还连接


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
