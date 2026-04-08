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
        df = con.execute(
            f"SELECT * FROM read_parquet('{_escape_path(path)}')"
        ).df()
    finally:
        con.close()

    if df.empty:
        return df

    columns = {str(col) for col in df.columns}
    if "ts" in columns:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    elif "date" in columns:
        df["ts"] = pd.to_datetime(df["date"], errors="coerce")
    elif "trade_date" in columns:
        df["ts"] = pd.to_datetime(df["trade_date"], errors="coerce")
    else:
        df["ts"] = pd.NaT

    if "date" not in columns:
        if "trade_date" in columns:
            df["date"] = df["trade_date"].astype(str).str[:10]
        else:
            df["date"] = pd.to_datetime(df["ts"], errors="coerce").dt.strftime("%Y-%m-%d")
    else:
        df["date"] = df["date"].astype(str).str[:10]

    if df["ts"].notna().any():
        df = df.sort_values("ts")
    elif "date" in df.columns:
        df = df.sort_values("date")
    return df.reset_index(drop=True)


def available_market_files() -> Iterable[Path]:
    ensure_storage_dirs()
    return sorted(MARKET_DIR.glob("*.parquet"))
