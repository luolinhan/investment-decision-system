"""
量化工作台配置
"""
import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("QW_DATA_DIR", BASE_DIR / "data" / "quant_workbench"))
MARKET_DIR = DATA_DIR / "market"
STATUS_FILE = DATA_DIR / "status.json"
INVESTMENT_DB_PATH = Path(os.getenv("INVESTMENT_DB_PATH", BASE_DIR / "data" / "investment.db"))
REPORTS_DB_PATH = Path(os.getenv("REPORTS_DB_PATH", BASE_DIR / "data" / "reports.db"))
LOG_PATH = BASE_DIR / "logs" / "quant_workbench.log"
APP_PORT = 8010
SCAN_ALL = os.getenv("QW_SCAN_ALL", "0") == "1"
SCAN_LIMIT = int(os.getenv("QW_SCAN_LIMIT", "400"))
