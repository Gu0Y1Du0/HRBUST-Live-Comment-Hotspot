from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_NAME: str = "HRBUST Monitor"
    # Redis 配置
    REDIS_HOST: str = "hadoop03"
    REDIS_PORT: int = 6379
    # kafka 配置
    KAFKA_BOOTSTRAP_SERVERS: str = "hadoop01:9092"

    # 项目根目录
    SERVER_HOME: str = "/home/hadoop/HRBUST-Live-Comment-Hotspot/"

    # 爬虫相关路径
    PYTHON_PATH: str = "/home/hadoop/HRBUST-Live-Comment-Hotspot/.venv/bin/python"
    SCRIPT_LIVE_BILIBILI: str = "collector/bili-collector.py"
    SCRIPT_VIDEO_BILIBILI: str = "collector/video_pipeline.py"
    SCRIPT_LIVE_DOUYIN: str = ""
    SCRIPT_VIDEO_DOUYIN: str = ""
    SCRIPT_LIVE_DOUYU: str = ""
    SCRIPT_VIDEO_DOUYU: str = ""

    class Config:
        env_file = ".env"


settings = Settings()
