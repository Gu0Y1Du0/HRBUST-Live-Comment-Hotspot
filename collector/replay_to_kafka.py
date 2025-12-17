import json
import time
from kafka import KafkaProducer

# 配置发送kafka的信息
KAFKA_BOOTSTRAP_SERVERS = ["hadoop01:9092"]
TOPIC_NAME = "danmaku_raw"
JSON_FILE_PATH = "video_danmaku.json"
SPEED = 20.0


def replay():
    # 初始化kafka发送者
    print("正在连接 Kafka...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    # 读取数据
    print(f"正在读取文件 {JSON_FILE_PATH}...")
    with open(JSON_FILE_PATH, "r", encoding="utf-8") as f:
        danmaku_list = json.load(f)

    # 确保按视频时间排序
    danmaku_list.sort(key=lambda x: x["video_time"])

    total_count = len(danmaku_list)
    print(f"准备就绪！共 {total_count} 条弹幕，即将以 {SPEED} 倍速重放...")
    print("3秒后开始...")
    time.sleep(3)

    # 记录"开始重放"的现实时间
    start_real_time = time.time()

    count = 0
    for dm in danmaku_list:
        # 视频里的相对时间
        video_time = dm.get("video_time", 0)

        # 这条弹幕应该在现实世界什么时刻进行发送
        # 目标时刻 = 开始时刻 + (视频进度 / 倍速)
        target_ts = start_real_time + (video_time / SPEED)

        # 当前现实时间
        current_ts = time.time()

        # 如果还没到发送时间就休息
        wait_seconds = target_ts - current_ts
        if wait_seconds > 0:
            time.sleep(wait_seconds)

        # 必须把时间戳篡改成现在的时间，不然flink会自主丢弃
        dm["ts"] = int(time.time() * 1000)

        room_id = str(dm.get("room_id", "unknown_room"))

        try:
            producer.send(TOPIC_NAME, value=dm, key=room_id)
            count += 1

            if count % 10 == 0:
                print(
                    f"[{count}/{total_count}] 已发送: {dm['content']} (延迟: {wait_seconds:.2f}s)"
                )

        except Exception as e:
            print(f"发送失败: {e}")

    print("重放结束")

    print("强制关闭Flink窗口，推送关闭信号")
    future_time = int(time.time() * 1000) + 60000

    end_msg = {
        "platform": "system",
        "room_id": "system_flush",
        "user": {"id": "0", "name": "system"},
        "content": "FLUSH",
        "event_type": "danmaku",
        "ts": future_time,
    }
    producer.send(TOPIC_NAME, value=end_msg)
    producer.flush()

    producer.close()


if __name__ == "__main__":
    replay()
