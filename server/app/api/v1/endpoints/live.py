import redis
from pydantic import BaseModel
from fastapi import APIRouter, Depends
from typing import List
from app.core.database import get_redis
from app.services.live_service import LiveService
from app.services.task_service import TaskService
from app.schemas.monitor import RankItem, HistoryResponse, VideoAnalyzeRequest

router = APIRouter()


class LiveMonitorRequest(BaseModel):
    room_id: str


# 开始监控直播状态
@router.post("/monitor/start")
def start_live_monitoring(request: LiveMonitorRequest):
    return TaskService.start_live_monitor(request.room_id)


# 开始监控视频重播状态，默认五倍速
@router.post("/analyze")
def analylze_video(request: VideoAnalyzeRequest):
    return TaskService.start_video_replay(request.bv_id)


# 获取当前监控房间的榜单
@router.get("/rank", response_model=List[RankItem])
def get_rank(r: redis.Redis = Depends(get_redis)):
    return LiveService.get_top_rooms(r)


# 获取当前房间的历史流数据
@router.get("/history/{room_id}", response_model=HistoryResponse)
def get_history(room_id: str, r: redis.Redis = Depends(get_redis)):
    data = LiveService.get_room_history(r, room_id)
    return HistoryResponse(room_id=room_id, data=data)
