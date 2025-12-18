import json
from typing import List, Any, cast
import redis
from app.schemas.monitor import RankItem


class LiveService:
    @staticmethod
    def get_top_rooms(r: redis.Redis, limit: int = 10) -> List[RankItem]:
        # 具体的 Redis 操作逻辑
        raw_data = r.zrevrange("current_hot_rooms", 0, limit - 1, withscores=True)
        data = cast(List[Any], raw_data)
        return [RankItem(room_id=item[0], heat=int(item[1])) for item in data]

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
