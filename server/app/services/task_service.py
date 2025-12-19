import subprocess
import redis
from app.core.config import settings
from fastapi import BackgroundTasks, HTTPException


class TaskService:
    # 辅助方法，执行命令并在结束后清理Redis锁
    @staticmethod
    def _run_process_and_unlock(cmd: list, lock_key: str, r: redis.Redis):
        try:
            # subprocess.run是阻塞的，他会一直等到脚本跑完
            # 而因为我们在BackgroundTask里运行，所以不会卡住主线程
            subprocess.run(cmd, check=True)
        except Exception as e:
            print(f"任务执行出错: {e}")
        finally:
            # 无论成功或者失败，最后都要删除锁，这样才能提交下次任务
            r.delete(lock_key)
            print(f"任务结束，已释放锁: {lock_key}")

    @staticmethod
    def start_live_monitor(room_id: str, r: redis.Redis):
        """
        调用bili-collector.py进行直播流监听
        """
        lock_key = f"task:live:{room_id}"

        # 查锁
        if r.exists(lock_key):
            # 直接抛出异常
            raise HTTPException(status_code=400, detail=f"直播间{room_id}已经在监控中!")

        # 加锁
        r.set(lock_key, "running")

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
    def start_video_replay(
        bv_id: str, r: redis.Redis, background_tasks: BackgroundTasks
    ):
        """
        调用video_pipeline.py 进行下载+重放
        """
        lock_key = f"task:video:{bv_id}"

        # 查锁
        if r.exists(lock_key):
            raise HTTPException(status_code=400, detail=f"视频{bv_id}已经在解析中!")

        # 加锁
        r.set(lock_key, "running", ex=120)

        cmd = [
            settings.PYTHON_PATH,
            settings.SCRIPT_VIDEO,
            "--bv",
            str(bv_id),
            "--speed",
            "5.0",
        ]

        try:
            background_tasks.add_task(
                TaskService._run_process_and_unlock, cmd, lock_key, r
            )
            return {
                "status": "success",
                "task_type": "video",
                "message": f"视频重播任务已启动, BV号: {bv_id}",
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}
