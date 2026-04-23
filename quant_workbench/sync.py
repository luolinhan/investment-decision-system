"""
量化工作台数据同步
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Dict, List

from quant_workbench.backtest import refresh_backtest_cache
from quant_workbench.config import SCAN_ALL, SCAN_LIMIT, STATUS_FILE
from quant_workbench.data_sources import YahooChartClient
from quant_workbench.storage import ensure_storage_dirs, parquet_path, write_parquet
from quant_workbench.universe import BENCHMARKS, WATCHLIST
from quant_workbench.universe_dynamic import load_a_share_universe


class QuantWorkbenchSync:
    def __init__(self) -> None:
        self.client = YahooChartClient()

    def run(self) -> Dict[str, int]:
        ensure_storage_dirs()
        counters = {"daily_files": 0, "intraday_files": 0, "benchmark_files": 0}
        errors: List[Dict[str, str]] = []

        universe = WATCHLIST
        if SCAN_ALL:
            universe = load_a_share_universe(limit=SCAN_LIMIT)

        for item in universe:
            self._sync_single(item.code, item.yahoo_symbol, counters, errors)

        for item in BENCHMARKS:
            try:
                daily = self.client.fetch_chart(item["yahoo_symbol"], "1d", "2y")
                if not daily.empty:
                    daily["code"] = item["code"]
                    write_parquet(daily, parquet_path(item["code"], "1d"))
                    counters["benchmark_files"] += 1
            except Exception as exc:
                errors.append(
                    {
                        "code": item["code"],
                        "symbol": item["yahoo_symbol"],
                        "scope": "benchmark",
                        "message": str(exc),
                    }
                )

        backtest_result = {"processed_codes": 0, "labels": 0, "stats": 0}
        try:
            backtest_result = refresh_backtest_cache(codes=[item.code for item in universe])
        except Exception as exc:
            errors.append(
                {
                    "code": "strategy_backtest",
                    "symbol": "local",
                    "scope": "backtest",
                    "message": str(exc),
                }
            )

        status = {
            "last_sync_at": datetime.now().isoformat(),
            **counters,
            "backtest_codes": backtest_result["processed_codes"],
            "signal_labels": backtest_result["labels"],
            "backtest_stats": backtest_result["stats"],
            "error_count": len(errors),
            "errors": errors[:10],
        }
        STATUS_FILE.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
        return counters

    def _sync_single(
        self,
        code: str,
        yahoo_symbol: str,
        counters: Dict[str, int],
        errors: List[Dict[str, str]],
    ) -> None:
        try:
            daily = self.client.fetch_chart(yahoo_symbol, "1d", "2y")
            if not daily.empty:
                daily["code"] = code
                write_parquet(daily, parquet_path(code, "1d"))
                counters["daily_files"] += 1
        except Exception as exc:
            errors.append(
                {"code": code, "symbol": yahoo_symbol, "scope": "1d", "message": str(exc)}
            )

        try:
            intraday = self.client.fetch_chart(yahoo_symbol, "5m", "60d")
            if not intraday.empty:
                intraday["code"] = code
                write_parquet(intraday, parquet_path(code, "5m"))
                counters["intraday_files"] += 1
        except Exception as exc:
            errors.append(
                {"code": code, "symbol": yahoo_symbol, "scope": "5m", "message": str(exc)}
            )
