from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path

import uvicorn


REPO_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = REPO_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_PATH = LOG_DIR / "uvicorn_service.log"


def main() -> None:
    os.chdir(REPO_DIR)
    if str(REPO_DIR) not in sys.path:
        sys.path.insert(0, str(REPO_DIR))
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    os.environ.setdefault("APP_ENV", "production")
    os.environ.setdefault("TZ", "Asia/Shanghai")
    os.environ.setdefault("RADAR_SNAPSHOT_ONLY", "1")
    os.environ.setdefault("INVESTMENT_NODE_ROLE", "windows_all_in_one")
    os.environ.setdefault("INVESTMENT_DATA_SOURCE_MODE", "windows_local")
    os.environ.setdefault("INVESTMENT_CONTROLLER_HOST", "windows-local")
    os.environ.setdefault("INVESTMENT_COLLECTOR_HOST", "windows-local")
    os.environ.setdefault("INVESTMENT_DB_PATH", str(REPO_DIR / "data" / "investment.db"))
    os.environ.setdefault("INVESTMENT_STORAGE_ROOT", str(REPO_DIR / "data"))

    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(f"starting windows_run_uvicorn_service.py pid={os.getpid()} cwd={REPO_DIR}\n")

    try:
        uvicorn.run("app.main:app", host="0.0.0.0", port=8080, access_log=False, log_config=None)
    except Exception:
        with LOG_PATH.open("a", encoding="utf-8") as fh:
            traceback.print_exc(file=fh)
        raise


if __name__ == "__main__":
    main()
