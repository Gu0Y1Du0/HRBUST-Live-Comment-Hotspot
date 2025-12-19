import asyncio
import argparse
import sys
import os
import json
import time
from kafka import KafkaProducer
from bilibili_api import video
import xml.etree.ElementTree as ET

from kafka.coordinator.assignors.sticky.sticky_assignor import (
    has_identical_list_elements,
)


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
    print(f"[{bv_id}] 正在获取视频信息...")
    v = video.Video(bvid=bv_id)
    pages = await v.get_pages()
    if not pages:
        raise Exception("未找到分集信息")

    cid = pages[0]["cid"]
    print(f"[{bv_id}] 正在下载弹幕XML(CID: {cid})...")
    xml_text = await v.get_danmaku_xml(cid=cid)

    messages = parse_xml(xml_text, bv_id)
    print(f"[{bv_id}] 解析完成，共{len(messages)}条弹幕")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False)
    return len(messages)


# 复用 replay_to_kafka.py 的逻辑
def replay_danmaku(json_file: str, kafka_servers: list, topic: str, speed: float = 2.0):
    print(f"正在连接Kafka: {kafka_servers}")
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    with open(json_file, "r", encoding="utf-8") as f:
        danmaku_list = json.load(f)

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

    producer.flush()
    producer.close()


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
        replay_danmaku(temp_file, ["hadoop01:9092"], "danmaku_raw", args.speed)

    except Exception as e:
        print(f"任务出错: {e}")
    finally:
        # 清理临时文件
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print(f"临时文件已清理: {temp_file}")
