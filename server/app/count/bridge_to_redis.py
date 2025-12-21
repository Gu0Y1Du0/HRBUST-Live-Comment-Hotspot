import json
import time
import redis
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv

# --- 配置 ---
current_dir = os.path.dirname(os.path.dirname(__file__))
env_path = os.path.join(current_dir, "../.env")
load_dotenv(env_path)

KAFKA_TOPIC = ["danmaku_agg", "danmaku_wordcloud"]
BOOTSTRAP_SERVERS = [os.getenv("KAFKA_BOOTSTRAP_SERVERS", "hadoop01:9092")]
REDIS_HOST = os.getenv("REDIS_HOST", "hadoop03")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


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
        *KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="bridge-group-v3",  # 独立的消费者组
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    print(f"正在监听 Kafka Topic: {KAFKA_TOPIC}...")

    # 搬运数据
    for message in consumer:
        try:
            data = message.value
            # data 格式: {"room_id": "732", "count": 19, "type": "agg"}

            room_id = str(data.get("room_id"))  # 确保转换成字符串
            count = data.get("count")

            print(f"收到消息: Topic={message.topic}, Type={data.get('type')}")

            # 修改数据类型
            msg_type = data.get("type", "agg")

            # 获取当前处理时间 (作为图表的 X 轴)
            # current_ts = int(time.time() * 1000)
            # 当前时间使用Kafka打上的时间戳，如果使用现在的时间戳会导致时间错误
            current_ts = message.timestamp

            # 如果kafka没打上时间戳才用现在的时间
            if current_ts is None:
                current_ts = int(time.time() * 1000)

            # --- 写入 Redis 逻辑 ---

            # 处理【热度统计】数据
            if msg_type == "agg":
                # 更新【当前热度】，用于“热度排行榜”
                # Key: "current_hot_rooms" (ZSET 有序集合，方便取 TopN)
                # Score: count, Member: room_id
                r.zadd("current_hot_rooms", {room_id: count})

                # 写入【历史趋势】，用于“前端画折线图”
                # Key: "history:732" (List 列表)
                # 我们只存最近 1 小时的数据 (3600秒)，防止 Redis 爆满
                history_key = f"history:{room_id}"

                should_write = True

                # 获取redis中最后一条数据
                last_entry_str = r.lindex(history_key, -1)

                if last_entry_str:
                    try:
                        last_entry = json.loads(str(last_entry_str))
                        last_ts = int(last_entry.get("ts", 0))

                        # 如果新数据比老数据还旧，就说明是kafka重发，丢弃即可
                        if current_ts <= last_ts:
                            should_write = False
                    except Exception as e:
                        print(e)

                # 通过检查才能写入
                if should_write:
                    # 更新排行榜
                    r.zadd("current_hot_rooms", {room_id: count})

                    point = json.dumps({"ts": current_ts, "value": count})

                    # 右边推入新数据
                    r.rpush(history_key, point)
                    # 左边弹出旧数据 (保持列表长度在 3600 以内)
                    if r.llen(history_key) > 3600:  # pyright:ignore
                        r.lpop(history_key)

                # print(f"[Redis Updated] Room: {room_id} | Count: {count} | Time: {current_ts}")
            elif msg_type == "wordcloud":
                # 数据格式
                word_list = data.get("data", [])
                wc_key = f"wordcloud:{room_id}"

                # 存入Redis String
                r.set(wc_key, json.dumps(word_list))
                print(f"词云已更新: {room_id} (包含 {len(word_list)} 个词)")
                # Key: "wordcloud:732"
            elif msg_type == "control":
                command = data.get("command")
                if command == "stop":
                    print(f"收到下线指令，清理房间: {room_id}")

                    # 从排行榜移除
                    r.zrem("current_hot_rooms", room_id)
        except Exception as e:
            print(f"数据处理出错: {e}")


if __name__ == "__main__":
    run_bridge()
