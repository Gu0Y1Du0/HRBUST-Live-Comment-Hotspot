import redis
from pydantic import BaseModel
from fastapi import APIRouter, Depends
from typing import List
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
