"""
Collect research reports for core pool and populate data/reports.db.
"""
from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# Disable all proxies to avoid AkShare proxy issues.
for proxy_var in [
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
]:
    os.environ.pop(proxy_var, None)
os.environ["NO_PROXY"] = "*"
os.environ["no_proxy"] = "*"

requests.Session.trust_env = False
_original_session = requests.Session


class NoProxySession(requests.Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trust_env = False
        self.proxies = {}


requests.Session = NoProxySession

import akshare as ak

INVESTMENT_DB = Path(__file__).resolve().parent / "data" / "investment.db"
REPORTS_DB = Path(__file__).resolve().parent / "data" / "reports.db"


def _normalize_code(raw: str, market: Optional[str] = None) -> str:
    raw = str(raw or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered.startswith(("sh", "sz")) and len(lowered) >= 8:
        return lowered[:8]
    if lowered.startswith("hk"):
        digits = re.sub(r"\D", "", lowered[2:])
        return f"hk{digits.zfill(5)}" if digits else "hk"
    if "." in raw:
        left, right = raw.split(".", 1)
        suffix = right.upper()
        left_digits = re.sub(r"\D", "", left)
        if suffix in {"SH", "SS"}:
            return f"sh{left_digits.zfill(6)}"
        if suffix == "SZ":
            return f"sz{left_digits.zfill(6)}"
        if suffix == "HK":
            return f"hk{left_digits.zfill(5)}"
    digits = re.sub(r"\D", "", raw)
    market_hint = (market or "").upper()
    if market_hint == "HK" or (not market_hint and len(digits) <= 5 and digits):
        return f"hk{digits.zfill(5)}"
    if len(digits) == 6:
        prefix = "sh" if digits.startswith(("5", "6", "9")) or digits.startswith("688") else "sz"
        return f"{prefix}{digits}"
    return lowered


def _ensure_reports_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            stock_code TEXT,
            stock_name TEXT,
            institution TEXT,
            author TEXT,
            rating TEXT,
            publish_date DATE,
            pdf_url TEXT,
            local_pdf_path TEXT,
            summary TEXT,
            raw_content TEXT,
            source TEXT,
            external_id TEXT UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reports_code ON reports(stock_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reports_date ON reports(publish_date)")


def _parse_date(value: str) -> Optional[date]:
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y年%m月%d日"):
        try:
            return datetime.strptime(value[:19], fmt).date()
        except Exception:
            continue
    return None


def _hash_external(parts: List[str]) -> str:
    text = "|".join(p for p in parts if p)
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def load_pool_members() -> List[Dict[str, Any]]:
    conn = sqlite3.connect(INVESTMENT_DB)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT member_code, member_name, member_market FROM stock_pool_constituents")
    rows = [dict(row) for row in c.fetchall()]
    conn.close()
    return rows


def collect_reports(
    days: int = 90,
    max_per_stock: int = 10,
    max_stocks: Optional[int] = None,
    commit_interval: int = 20,
) -> Dict[str, Any]:
    cutoff = date.today() - timedelta(days=days)
    members = load_pool_members()
    a_codes: Dict[str, str] = {}
    for item in members:
        code = _normalize_code(item.get("member_code"), item.get("member_market"))
        if code.startswith(("sh", "sz")):
            a_codes[code[2:]] = item.get("member_name") or ""

    conn = sqlite3.connect(REPORTS_DB)
    _ensure_reports_table(conn)
    c = conn.cursor()

    inserted = 0
    processed = 0
    for symbol, name in a_codes.items():
        if max_stocks is not None and processed >= max_stocks:
            break
        try:
            df = ak.stock_research_report_em(symbol=symbol)
        except Exception as exc:
            print(f"[reports] {symbol} failed: {exc}")
            continue
        if df is None or df.empty:
            continue

        cols = list(df.columns)
        per_stock = 0
        for idx, row in df.iterrows():
            values = row.values
            title = ""
            pdf_url = ""
            stock_code_val = f"{symbol}.SH" if symbol.startswith(("6", "5", "9")) else f"{symbol}.SZ"
            stock_name_val = name
            institution = ""
            rating = ""
            publish_date = None

            for i, col in enumerate(cols):
                col_lower = str(col).lower()
                val = str(values[i]) if i < len(values) else ""
                if "标题" in col or "title" in col_lower or "报告名称" in col:
                    title = val
                elif "pdf" in col_lower or "链接" in col:
                    pdf_url = val
                elif "股票代码" in col or "code" in col_lower:
                    stock_code_val = val
                elif "股票简称" in col or "名称" in col:
                    stock_name_val = val
                elif "机构" in col:
                    institution = val
                elif "类型" in col or "评级" in col:
                    rating = val
                elif "日期" in col or "date" in col_lower:
                    publish_date = _parse_date(val)

            if not title or not publish_date or publish_date < cutoff:
                continue

            external_id = f"ak_{symbol}_{_hash_external([title, str(publish_date), institution])}"
            c.execute(
                """
                INSERT OR IGNORE INTO reports
                (title, stock_code, stock_name, institution, author, rating, publish_date,
                 pdf_url, local_pdf_path, summary, raw_content, source, external_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                """,
                (
                    title,
                    stock_code_val,
                    stock_name_val,
                    institution,
                    "",
                    rating,
                    publish_date.isoformat(),
                    pdf_url if pdf_url.startswith("http") else "",
                    "eastmoney",
                    external_id,
                ),
            )
            if c.rowcount:
                inserted += 1
                per_stock += 1
            if per_stock >= max_per_stock:
                break
        time.sleep(0.1)
        processed += 1
        if processed % commit_interval == 0:
            conn.commit()

    conn.commit()
    conn.close()
    return {"inserted": inserted, "stocks": processed}


def main() -> None:
    result = collect_reports()
    print(result)


if __name__ == "__main__":
    main()
