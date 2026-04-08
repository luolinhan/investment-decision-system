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
    foreign_research_root: str = "./data/foreign_research"
    foreign_research_raw_path: str = "./data/foreign_research/raw"
    foreign_research_text_path: str = "./data/foreign_research/text"
    foreign_research_analysis_path: str = "./data/foreign_research/analysis"
    foreign_research_manifest_path: str = "./data/foreign_research/manifests"
    foreign_research_retention_days: int = 180

    # 慧博投研
    huibor_username: Optional[str] = None
    huibor_password: Optional[str] = None

    # Claude API
    anthropic_api_key: Optional[str] = None
    bailian_api_key: Optional[str] = None
    bailian_base_url: str = "https://coding.dashscope.aliyuncs.com/v1"
    foreign_research_model: str = "glm-5"
    foreign_research_timeout_seconds: int = 45

    # Task-06: SSL验证配置（默认开启，Windows证书问题时可显式关闭）
    http_verify_ssl: bool = True

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
        "data/foreign_research",
        "data/foreign_research/raw",
        "data/foreign_research/text",
        "data/foreign_research/analysis",
        "data/foreign_research/manifests",
        "logs",
        "static/css",
        "static/js",
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)


settings = Settings()
