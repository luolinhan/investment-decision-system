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
    investment_db_path: str = "./data/investment.db"
    investment_storage_root: str = "./data"

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
    investment_db_dir = os.path.dirname(settings.investment_db_path) or "."
    dirs = [
        "data",
        "data/pdfs",
        "logs",
        "static/css",
        "static/js",
        settings.investment_storage_root,
        investment_db_dir,
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def get_investment_runtime_profile() -> dict:
    """
    Return machine-readable values and human-readable labels describing
    how the investment node is deployed.

    Defaults reflect the simplified production target:
      - Windows is the serving, data, and storage node
      - Mac is only a development/control machine
      - Aliyun is optional and should not be required for runtime acceptance
    """
    node_role = os.getenv("INVESTMENT_NODE_ROLE", "windows_all_in_one")
    data_source = os.getenv("INVESTMENT_DATA_SOURCE_MODE", "windows_local")
    controller_host = os.getenv("INVESTMENT_CONTROLLER_HOST", "windows-local")
    collector_host = os.getenv("INVESTMENT_COLLECTOR_HOST", "windows-local")

    role_labels = {
        "windows_all_in_one": "Windows 本地一体化节点",
        "windows_service": "Windows 服务节点",
        "mac_controller": "Mac 控制节点",
        "aliyun_collector": "阿里云采集节点",
    }
    mode_labels = {
        "windows_local": "Windows 本地 SQLite/DuckDB",
        "snapshot_first": "快照优先 (SQLite)",
        "realtime_only": "纯实时",
        "hybrid": "混合模式 (快照+实时)",
    }

    return {
        "node_role": node_role,
        "node_role_label": role_labels.get(node_role, node_role),
        "data_source_mode": data_source,
        "data_source_mode_label": mode_labels.get(data_source, data_source),
        "controller_host": controller_host,
        "collector_host": collector_host,
        "investment_db_path": settings.investment_db_path,
        "investment_storage_root": settings.investment_storage_root,
    }


settings = Settings()
