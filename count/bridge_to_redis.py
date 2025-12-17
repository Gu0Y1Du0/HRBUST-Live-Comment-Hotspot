import json
import time
import redis
from kafka import KafkaConsumer

# --- 配置 ---
KAFKA_TOPIC = "danmaku_agg"
BOOTSTRAP_SERVERS = ["hadoop01:9092"]
REDIS_HOST = "hadoop03"
REDIS_PORT = 6379


def run_bridge():
    # 连接 Redis
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        print(f"成功连接到 Redis ({REDIS_HOST})")
    except Exception as e:
        print(f"Redis 连接失败: {e}")
        return

    # 连接 Kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="dashboard_writer_group",  # 独立的消费者组
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    print(f"正在监听 Kafka Topic: {KAFKA_TOPIC}...")

    # 搬运数据
    for message in consumer:
        data = message.value
        # data 格式: {"room_id": "732", "count": 19, "type": "agg"}

        room_id = data.get("room_id")
        count = data.get("count")

        # 获取当前处理时间 (作为图表的 X 轴)
        current_ts = int(time.time() * 1000)

        # --- 写入 Redis 逻辑 ---

        # 更新【当前热度】，用于“热度排行榜”
        # Key: "current_hot_rooms" (ZSET 有序集合，方便取 TopN)
        # Score: count, Member: room_id
        r.zadd("current_hot_rooms", {room_id: count})

        # B. 写入【历史趋势】，用于“前端画折线图”
        # Key: "history:732" (List 列表)
        # 我们只存最近 1 小时的数据 (3600秒)，防止 Redis 爆满
        history_key = f"history:{room_id}"
        point = json.dumps({"ts": current_ts, "value": count})

        # 右边推入新数据
        r.rpush(history_key, point)
        # 左边弹出旧数据 (保持列表长度在 3600 以内)
        if r.llen(history_key) > 3600:  # pyright:ignore
            r.lpop(history_key)

        print(f"[Redis Updated] Room: {room_id} | Count: {count} | Time: {current_ts}")


if __name__ == "__main__":
    run_bridge()
