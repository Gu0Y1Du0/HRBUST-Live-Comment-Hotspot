import asyncio
import argparse
import sys
import redis
import os
import json
import time
from kafka import KafkaProducer
from bilibili_api import video
import xml.etree.ElementTree as ET

from kafka.coordinator.assignors.sticky.sticky_assignor import (
    has_identical_list_elements,
)
from dotenv import load_dotenv
from app.core.database import pool

# --- 配置 ---
current_dir = os.path.dirname(os.path.dirname(__file__))
env_path = os.path.join(current_dir, "../.env")
load_dotenv(env_path)

BOOTSTRAP_SERVERS = [os.getenv("KAFKA_BOOTSTRAP_SERVERS", "hadoop01:9092")]
REDIS_HOST = os.getenv("REDIS_HOST", "hadoop03")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


# 复用fet_replay.py逻辑
def _safe_int(x: str, default: int = 0) -> int:
    try:
        return int(float(x))
    except:
        return default


def parse_xml(xml_text: str, room_id: str) -> list:
    root = ET.fromstring(xml_text)
    out = []
    for d in root.findall(".//d"):
        p = d.attrib.get("p", "").split(",")
        text = (d.text or "").strip()
        if not text:
            continue

        appear_s = float(p[0]) if len(p) > 0 else 0.0
        # 原始发送时间
        send_ts_s = p[4] if len(p) > 4 else "0"
        ts_ms = (
            _safe_int(send_ts_s) * 1000
            if _safe_int(send_ts_s) > 0
            else int(time.time() * 1000)
        )

        out.append(
            {
                "platform": "bilibili",
                "room_id": room_id,
                "user": {"id": p[6] if len(p) > 6 else "unknown", "name": None},
                "content": text,
                "event_type": "danmaku",
                "ts": ts_ms,
                "video_time": appear_s,
            }
        )
    out.sort(key=lambda x: x["video_time"])
    return out


async def fetch_danmaku(bv_id: str, output_file: str):
    print(f"[{bv_id}]正在获取视频信息...")
    v = video.Video(bvid=bv_id)
    pages = await v.get_pages()
    if not pages:
        raise Exception("未找到分集信息")

    cid = pages[0]["cid"]
    print(f"[{bv_id}] 正在下载弹幕XML(CID: {cid})...")
    xml_text = await v.get_danmaku_xml(cid=cid)

    # 这里加上bilibili的前缀
    unique_bv_id = f"bilibili_video:{bv_id}"

    messages = parse_xml(xml_text, unique_bv_id)
    print(f"[{bv_id}] 解析完成，共{len(messages)}条弹幕")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False)
    return len(messages)


# 复用 replay_to_kafka.py 的逻辑
def replay_danmaku(json_file: str, kafka_servers: list, topic: str, speed: float = 2.0):
    r = redis.Redis(connection_pool=pool)

    print(f"正在连接Kafka: {kafka_servers}")
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    with open(json_file, "r", encoding="utf-8") as f:
        danmaku_list = json.load(f)

    if not danmaku_list:
        print("弹幕列表为空，退出")
        r.close()
        return

    current_room_id = danmaku_list[0].get("room_id")

    if current_room_id:
        print(f"更新redis状态: {current_room_id} -> RUNNING")
        r.hset("monitor:task_status", current_room_id, "RUNNING")

    start_real_time = time.time()
    total = len(danmaku_list)
    print(f"开始重放，倍速: {speed}, 总数: {total}")

    for i, dm in enumerate(danmaku_list):
        video_time = dm.get("video_time", 0)
        target_ts = start_real_time + (video_time / speed)
        current_ts = time.time()

        wait = target_ts - current_ts
        if wait > 0:
            time.sleep(wait)

        # 篡改时间戳为当前时间
        dm["ts"] = int(time.time() * 1000)

        try:
            producer.send(topic, value=dm, key=dm["room_id"])
            if i % 50 == 0:
                print(f"[{i}/{total}] 发送: {dm['content']}")
        except Exception as e:
            print(f"发送失败: {e}")

    print("重放结束")

    print("强制关闭Flink窗口，推送关闭信号")
    future_time = int(time.time() * 1000) + 60000

    end_msg = {
        "platform": "system",
        "room_id": "system_flush",
        "user": {
            "id": "0",
            "name": "system",
        },
        "content": "FLUSH",
        "event_type": "danmaku",
        "ts": future_time,
    }

    # 发送这条结束信号
    try:
        producer.send(topic, value=end_msg)
        print(f"已发送FLUSH信号，时间戳: {future_time}")
    except Exception as e:
        print(f"FLUSH信号发送失败: {e}")

    # 直接发给danmaku_agg, 让Bridge收到后删除Redis里面的数据，防止多条线重复出现在折线图当中
    # 从json_list的第一条数据里拿room_id(带前缀消息的)
    current_room_id = danmaku_list[0].get("room_id")

    if current_room_id:
        stop_msg = {
            "type": "control",  # 新的消息类型
            "command": "stop",  # 停止
            "room_id": current_room_id,  # 删除哪个房间
        }
        print(f"发送下线信号: {current_room_id}")
        # 发送给danmaku_agg
        producer.send("danmaku_agg", value=stop_msg)

    producer.flush()
    producer.close()
    r.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bv", required=True, help="BV号")
    parser.add_argument(
        "--speed", type=float, default=5.0, help="重放倍速"
    )  # 默认五倍速
    args = parser.parse_args()

    # 生成一个唯一的文件名，防止冲突
    temp_file = f"temp_danmaku_{args.bv}.json"

    try:
        # 下载
        asyncio.run(fetch_danmaku(args.bv, temp_file))

        # 重放
        replay_danmaku(temp_file, BOOTSTRAP_SERVERS, "danmaku_raw", args.speed)

    except Exception as e:
        print(f"任务出错: {e}")
    finally:
        # 清理临时文件
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print(f"临时文件已清理: {temp_file}")
