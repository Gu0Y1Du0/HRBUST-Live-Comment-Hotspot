from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Any, cast
import redis
import json

app = FastAPI()

# 配置CORS(允许前端跨域访问)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境改成具体域名，开发环境可以用 *
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 连接Redis
# 注意：如果是本机开发连服务器，host要填服务器IP；如果是服务器内部跑，用hadoop03
REDIS_HOST = "hadoop03"
REDIS_PORT = 6379
r: redis.Redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


@app.get("/")
def read_root():
    return {"message": "HRBUST Live Monitor API is running!"}


@app.get("/api/rank")
def get_rank():
    """获取当前热度排行榜 (Top 10)"""
    # ZREVRANGE: 按分数从大到小取
    raw_data = r.zrevrange("current_hot_rooms", 0, 9, withscores=True)
    data = cast(List[Any], raw_data)
    # data 格式: [('room_id', score), ...]
    result = [{"room_id": item[0], "heat": int(item[1])} for item in data]
    return result


@app.get("/api/history/{room_id}")
def get_history(room_id: str):
    """获取指定房间的历史趋势"""
    key = f"history:{room_id}"
    # LRANGE: 获取列表所有数据
    raw_response = r.lrange(key, 0, -1)
    raw_list = cast(List[Any], raw_response)

    # 解析 JSON 字符串
    history_data = []
    for item in raw_list:
        try:
            point = json.loads(item)
            history_data.append(point)
        except:
            pass

    return {"room_id": room_id, "data": history_data}


if __name__ == "__main__":
    # host="0.0.0.0" 允许局域网访问
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
