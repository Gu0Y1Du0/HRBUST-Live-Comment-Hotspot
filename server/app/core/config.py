import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    project_name: str = "HRBUST Monitor"
    # Redis 配置
    redis_host: str = "hadoop03"
    redis_port: int = 6379
    redis_password: str = ""
    # kafka 配置
    kafka_bootstrap_servers: str = "hadoop01:9092"

    # 项目根目录
    base_dir: str = "/home/hadoop/HRBUST-Live-Comment-Hotspot"
    python_path: str = f"{base_dir}/.venv/bin/python"

    # 爬虫相关路径
    script_live_bilibili: str = "server/collector/bili-collector.py"
    script_video_bilibili: str = "server/collector/video_pipeline.py"
    script_live_douyin: str = ""
    script_video_douyin: str = ""
    script_live_douyu: str = ""
    script_video_douyu: str = ""

    class Config:
        # env_file = ".env"
        env_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env"
        )
        env_file_encoding = "utf-8"


settings = Settings()
