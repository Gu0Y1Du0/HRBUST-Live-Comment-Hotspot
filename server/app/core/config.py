import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv


# --- 配置 ---
current_dir = os.path.dirname(os.path.dirname(__file__))
env_path = os.path.join(current_dir, "../.env")
load_dotenv(env_path)
BASE_DIR = os.getenv("BASE_DIR", "/home/soyor1n/HRBUSTLinux/HRBUST-Live-Comment-Hotspot/")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "hadoop01:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "hadoop02")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")


class Settings(BaseSettings):
    project_name: str = "HRBUST Monitor"
    # Redis 配置
    redis_host: str = REDIS_HOST
    redis_port: int = int(REDIS_PORT)
    redis_password: str = REDIS_PASSWORD
    # kafka 配置
    kafka_bootstrap_servers: str = BOOTSTRAP_SERVERS 

    # 项目根目录
    base_dir: str = BASE_DIR
    python_path: str = f"{base_dir}/.venv/bin/python"

    # 爬虫相关路径
    script_live_bilibili: str = "server/collector/bili-collector.py"
    script_video_bilibili: str = "server/collector/video_pipeline.py"
    script_live_douyin: str = "server/collector/douyin-collector.py"
    script_video_douyin: str = ""
    script_live_douyu: str = "server/collector/douyu_collector_sync.py"
    script_video_douyu: str = ""

    class Config:
        # env_file = ".env"
        env_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env"
        )
        env_file_encoding = "utf-8"


settings = Settings()
