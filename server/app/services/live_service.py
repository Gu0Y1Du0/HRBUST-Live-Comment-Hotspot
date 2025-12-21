import json
from typing import List, Any, cast
import redis
from app.schemas.monitor import RankItem


class LiveService:
    @staticmethod
    def get_top_rooms(r: redis.Redis, limit: int = 10) -> List[RankItem]:
        # 具体的 Redis 操作逻辑
        # 先从ZSET获取排行前N的房间ID和热度
        raw_data = r.zrevrange("current_hot_rooms", 0, limit - 1, withscores=True)
        data = cast(List[Any], raw_data)

        if not data:
            return []

        # 提取所有的room_id
        room_ids = [item[0] for item in data]

        # 从Redis Hash中批量获取这些房间的状态
        # 假设Hash Key叫"monitor:task_status",格式{"bilibili:room_id":"RUNNING"}
        # 如果Redis里没有状态，默认给'RUNNING'以防报错
        raw_statused = cast(List[Any], r.hmget("monitor:task_status", room_ids))

        result = []
        for i, (room_id, score) in enumerate(data):
            # 处理Redis返回None的情况
            status_str = raw_statused[i]
            if status_str is None:
                # 默认状态，视业务情况而定，这里暂定RUNNING
                current_status = "RUNNING"
            else:
                # redis返回的是bytes或str
                current_status = (
                    status_str
                    if isinstance(status_str, str)
                    else status_str.decode("utf-8")
                )

            result.append(
                RankItem(room_id=room_id, heat=int(score), status=current_status)
            )

        return result

    @staticmethod
    def get_room_history(r: redis.Redis, room_id: str) -> List[Any]:
        key = f"history:{room_id}"
        raw_response = r.lrange(key, 0, -1)
        raw_list = cast(List[Any], raw_response)
        history_data = []
        for item in raw_list:
            try:
                history_data.append(json.loads(item))
            except:
                continue
        return history_data
