import redis
from app.core.config import settings

# 创建连接池
pool = redis.ConnectionPool(
    host=settings.REDIS_HOST, port=settings.REDIS_PORT, decode_response=True
)


# 依赖注入函数
def get_redis():
    r = redis.Redis(connection_pool=pool)
    try:
        yield r
    finally:
        r.close()  # 归还链接给池子
