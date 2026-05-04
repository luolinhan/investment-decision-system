from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
LOG_DIR = REPO_ROOT / "logs"
LOG_PATH = LOG_DIR / "uvicorn_service.log"


def _log(message: str) -> None:
    LOG_DIR.mkdir(exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(f"{message}\n")


def main() -> None:
    os.chdir(REPO_ROOT)
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    os.environ.setdefault("APP_ENV", "production")
    os.environ.setdefault("TZ", "Asia/Shanghai")
    os.environ.setdefault("RADAR_SNAPSHOT_ONLY", "1")
    os.environ.setdefault("INVESTMENT_NODE_ROLE", "windows_all_in_one")
    os.environ.setdefault("INVESTMENT_DATA_SOURCE_MODE", "windows_local")
    os.environ.setdefault("INVESTMENT_CONTROLLER_HOST", "windows-local")
    os.environ.setdefault("INVESTMENT_COLLECTOR_HOST", "windows-local")
    os.environ.setdefault("INVESTMENT_DB_PATH", str(REPO_ROOT / "data" / "investment.db"))
    os.environ.setdefault("INVESTMENT_STORAGE_ROOT", str(REPO_ROOT / "data"))

    _log(f"starting uvicorn_service.py pid={os.getpid()} cwd={REPO_ROOT}")

    try:
        import uvicorn

        uvicorn.run(
            "app.main:app",
            host="0.0.0.0",
            port=8080,
            workers=1,
            access_log=False,
            log_config=None,
        )
    except Exception:
        with LOG_PATH.open("a", encoding="utf-8") as fh:
            traceback.print_exc(file=fh)
        raise


if __name__ == "__main__":
    main()
