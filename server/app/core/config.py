from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_NAME: str = "HRBUST Monitor"
    # Redis 配置
    REDIS_HOST: str = "hadoop03"
    REDIS_PORT: int = 6379
    # kafka 配置
    KAFKA_BOOTSTRAP_SERVERS: str = "hadoop01:9092"

    # 爬虫相关路径
    PYTHON_PATH: str = "/home/hadoop/HRBUST-Live-Comment-Hotspot/.venv/bin/python"
    SCRIPT_LIVE: str = "collector/bili-collector.py"
    SCRIPT_VIDEO: str = "collector/video_pipeline.py"

    class Config:
        env_file = ".env"


settings = Settings()
