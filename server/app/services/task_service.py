import subprocess
from app.core.config import settings


class TaskService:
    @staticmethod
    def start_live_monitor(room_id: str):
        """
        调用bili-collector.py进行直播流监听
        """
        cmd = [settings.PYTHON_PATH, settings.SCRIPT_LIVE, "--room-id", str(room_id)]

        try:
            subprocess.Popen(cmd)
            return {
                "status": "success",
                "task_type": "live",
                "message": f"直播监控已启动，房间号: {room_id}",
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @staticmethod
    def start_video_replay(bv_id: str):
        """
        调用video_pipeline.py 进行下载+重放
        """
        cmd = [
            settings.PYTHON_PATH,
            settings.SCRIPT_VIDEO,
            "--bv",
            str(bv_id),
            "--speed",
            "5.0",
        ]

        try:
            subprocess.Popen(cmd)
            return {
                "status": "success",
                "task_type": "video",
                "message": f"视频重播任务已启动, BV号: {bv_id}",
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}
