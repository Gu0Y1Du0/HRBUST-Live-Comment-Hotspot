from pydantic import BaseModel
from typing import List, Any


# 直播监控请求
class LiveMonitorRequest(BaseModel):
    room_id: str
    platform: str  # bilibili | douyin | douyu


# 视频解析请求
class VideoAnalyzeRequest(BaseModel):
    bv_id: str


# 排行榜单项
class RankItem(BaseModel):
    room_id: str
    heat: int


# 历史趋势响应
class HistoryResponse(BaseModel):
    room_id: str
    data: List[Any]
