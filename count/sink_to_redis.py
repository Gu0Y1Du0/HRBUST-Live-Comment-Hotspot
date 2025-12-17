import redis
from pyflink.datastream.functions import MapFunction


class RedisSink(MapFunction):
    def __init__(self, host="hadoop02", port=6379, db=0):
        self.host = host
        self.port = port
        self.db = self.db
        self.redis = None

    def open(self, runtime_context):
        try:
            print("正在建立Redis连接......")
            self.redis = redis.Redis(
                host=self.host, port=self.port, db=self.db, decode_responses=True
            )
        except Exception as e:
            print(f"Redis连接失败: {e}")

    def map(self, value):
        if self.redis is None:
            return value

        try:
            # 你的写入逻辑
            key = f"hotspot:{value['platform']}:{value['room_id']}"
            self.redis.hset(
                key,
                mapping={
                    "count": str(value["count"]),
                    "window_end": str(value["window_end"]),
                },
            )
            self.redis.expire(key, 3600)
        except Exception as e:
            print(f"写入失败: {e}")

        return value

    def close(self):
        if self.redis:
            self.redis.close()
