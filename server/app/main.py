import redis
from typing import Awaitable
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1.endpoints import live, video
from app.core.database import pool
from app.services.task_service import TaskService

app = FastAPI(title="HRBUST Data Monitor")

# CORS配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境改成具体域名，开发环境可以用 *
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
# 直播路由
app.include_router(live.router, prefix="/api/live", tags=["直播监控"])
# 视频路由
app.include_router(video.router, prefix="/api/video", tags=["视频重播"])


@app.get("/")
def root():
    return {"msg": "Server is running properly with new structure!"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("系统启动中... 正在检查需要恢复的直播监控任务...")
    r = redis.Redis(connection_pool=pool)

    try:
        config_key = "sys:config:live_rooms"
        saved_items = r.smembers(config_key)

        if isinstance(saved_items, Awaitable):
            saved_items = await saved_items

        if saved_items:
            print(f"发现{len(saved_items)}个历史任务")
            for item in saved_items:
                try:
                    # 解析platform:room_id
                    if ":" in item:
                        platform, room_id = item.split(":", 1)
                        # 清理旧锁
                        lock_key = f"task:live:{platform}:{room_id}"
                        r.delete(lock_key)

                        # 恢复任务
                        print(f"正在恢复: {platform} - {room_id}")
                        TaskService.start_live_monitor(room_id, platform, r)
                    else:
                        print(f"跳过格式错误的数据: {item}")

                except Exception as e:
                    print(f"恢复任务{item}失败:{e}")
        else:
            print("没有发现历史任务，系统干净启动。")

    except Exception as e:
        print(f"自动恢复任务出错: {e}")
    finally:
        # 这里的 close 只是把连接“归还”给池子，不会关闭整个池子，非常安全
        r.close()

    yield

    print("系统正在关闭...")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
