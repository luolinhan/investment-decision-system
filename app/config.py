"""
研报下载系统 - 配置管理
"""
from pydantic_settings import BaseSettings
from typing import Optional
import os


class Settings(BaseSettings):
    # 服务器配置
    host: str = "0.0.0.0"
    port: int = 8080

    # 数据库
    database_url: str = "sqlite+aiosqlite:///./data/reports.db"

    # PDF存储
    pdf_storage_path: str = "./data/pdfs"

    # 慧博投研
    huibor_username: Optional[str] = None
    huibor_password: Optional[str] = None

    # Claude API
    anthropic_api_key: Optional[str] = None

    # 定时任务
    scheduler_hour: int = 6
    scheduler_minute: int = 0

    # 日志
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# 创建必要的目录
def ensure_directories():
    dirs = [
        "data",
        "data/pdfs",
        "logs",
        "static/css",
        "static/js",
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)


settings = Settings()