"""
Parquet / DuckDB 存储工具
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from quant_workbench.config import DATA_DIR, MARKET_DIR


def ensure_storage_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    MARKET_DIR.mkdir(parents=True, exist_ok=True)


def _escape_path(path: Path) -> str:
    return str(path).replace("'", "''")


def parquet_path(code: str, interval: str) -> Path:
    ensure_storage_dirs()
    safe_code = code.replace("/", "_")
    return MARKET_DIR / f"{safe_code}_{interval}.parquet"


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    import duckdb

    ensure_storage_dirs()
    if path.exists():
        path.unlink()

    con = duckdb.connect(database=":memory:")
    try:
        con.register("frame_view", df)
        con.execute(
            f"COPY frame_view TO '{_escape_path(path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()


def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    import duckdb

    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(
            f"SELECT * FROM read_parquet('{_escape_path(path)}') ORDER BY ts"
        ).df()
    finally:
        con.close()


def available_market_files() -> Iterable[Path]:
    ensure_storage_dirs()
    return sorted(MARKET_DIR.glob("*.parquet"))
