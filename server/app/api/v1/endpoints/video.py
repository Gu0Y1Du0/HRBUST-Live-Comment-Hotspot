import redis
from fastapi import APIRouter, BackgroundTasks, Depends
from app.schemas.monitor import VideoAnalyzeRequest
from app.services.task_service import TaskService
from server.app.core.database import get_redis


router = APIRouter()


# 开始监控视频重播状态，默认五倍速
@router.post("/analyze")
def analylze_video(
    request: VideoAnalyzeRequest,
    background_tasks: BackgroundTasks,
    r: redis.Redis = Depends(get_redis),
):
    return TaskService.start_video_replay(request.bv_id, r, background_tasks)
