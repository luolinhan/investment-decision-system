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

    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write("starting windows_run_uvicorn_service.py\n")

    try:
        uvicorn.run("app.main:app", host="0.0.0.0", port=8080, access_log=False, log_config=None)
    except Exception:
        with LOG_PATH.open("a", encoding="utf-8") as fh:
            traceback.print_exc(file=fh)
        raise


if __name__ == "__main__":
    main()
