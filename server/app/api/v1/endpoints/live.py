import redis
import json
from pydantic import BaseModel
from fastapi import APIRouter, Depends
from typing import List, cast
from app.core.database import get_redis
from app.services.live_service import LiveService
from app.services.task_service import TaskService
from app.schemas.monitor import (
    RankItem,
    HistoryResponse,
    VideoAnalyzeRequest,
    LiveMonitorRequest,
)

router = APIRouter()


# 开始监控直播状态
@router.post("/monitor/start")
def start_live_monitoring(
    request: LiveMonitorRequest, r: redis.Redis = Depends(get_redis)
):
    return TaskService.start_live_monitor(request.room_id, request.platform, r)


# 获取当前监控房间的榜单
@router.get("/rank", response_model=List[RankItem])
def get_rank(r: redis.Redis = Depends(get_redis)):
    return LiveService.get_top_rooms(r)


# 获取当前房间的历史流数据
@router.get("/history/{room_id}", response_model=HistoryResponse)
def get_history(room_id: str, r: redis.Redis = Depends(get_redis)):
    data = LiveService.get_room_history(r, room_id)
    return HistoryResponse(room_id=room_id, data=data)


# 获取当前的词云
@router.get("/monitor/wordcloud")
def get_word_cloud(room_id: str, r: redis.Redis = Depends(get_redis)):
    """
    获取指定房间的实时词云数据
    """
    # 在Bridge里面存的是"wordcloud:bilibili:732"
    # 前端传过来的已经是带前缀的了
    print(f"前端请求词云: room_id={room_id}")
    key = f"wordcloud:{room_id}"

    data = r.get(key)

    if not data:
        return []

    try:
        # Redis里面存储的是字符串，我们需要把他还原成Json对象列表
        json_str = cast(str, data)
        return json.loads(json_str)
    except:
        return []
