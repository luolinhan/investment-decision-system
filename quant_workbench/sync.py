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
from quant_workbench.storage import available_market_files, ensure_storage_dirs, parquet_path, write_parquet
from quant_workbench.universe import BENCHMARKS, WATCHLIST
from quant_workbench.universe_dynamic import load_a_share_universe


class QuantWorkbenchSync:
    def __init__(self) -> None:
        self.client = YahooChartClient()
        self.yahoo_forbidden_count = 0
        self.yahoo_blocked = False

    def run(self) -> Dict[str, int]:
        ensure_storage_dirs()
        counters = {
            "daily_files": 0,
            "intraday_files": 0,
            "benchmark_files": 0,
            "reused_daily_files": 0,
            "reused_intraday_files": 0,
            "skipped_symbols": 0,
        }
        errors: List[Dict[str, str]] = []
        error_total = 0

        universe = WATCHLIST
        if SCAN_ALL:
            universe = load_a_share_universe(limit=SCAN_LIMIT)

        for item in universe:
            error_total += self._sync_single(item.code, item.yahoo_symbol, counters, errors)

        for item in BENCHMARKS:
            path = parquet_path(item["code"], "1d")
            if self.yahoo_blocked:
                counters["skipped_symbols"] += 1
                if path.exists():
                    counters["reused_daily_files"] += 1
                continue
            try:
                daily = self.client.fetch_chart(item["yahoo_symbol"], "1d", "2y")
                if not daily.empty:
                    daily["code"] = item["code"]
                    write_parquet(daily, parquet_path(item["code"], "1d"))
                    counters["benchmark_files"] += 1
            except Exception as exc:
                error_total += 1
                self._record_error(
                    errors,
                    {
                        "code": item["code"],
                        "symbol": item["yahoo_symbol"],
                        "scope": "benchmark",
                        "message": str(exc),
                    },
                )
                if self._is_forbidden_error(exc):
                    self.yahoo_forbidden_count += 1
                    self._update_yahoo_blocked()
                    if path.exists():
                        counters["reused_daily_files"] += 1

        backtest_result = {"processed_codes": 0, "labels": 0, "stats": 0}
        try:
            backtest_result = refresh_backtest_cache(codes=[item.code for item in universe])
        except Exception as exc:
            error_total += 1
            self._record_error(
                errors,
                {
                    "code": "strategy_backtest",
                    "symbol": "local",
                    "scope": "backtest",
                    "message": str(exc),
                },
            )

        market_files = list(available_market_files())
        available_daily_files = sum(1 for path in market_files if path.name.endswith("_1d.parquet"))
        available_intraday_files = sum(1 for path in market_files if path.name.endswith("_5m.parquet"))

        status = {
            "last_sync_at": datetime.now().isoformat(),
            **counters,
            "backtest_codes": backtest_result["processed_codes"],
            "signal_labels": backtest_result["labels"],
            "backtest_stats": backtest_result["stats"],
            "error_count": error_total,
            "errors": errors[:10],
            "yahoo_blocked": self.yahoo_blocked,
            "yahoo_forbidden_count": self.yahoo_forbidden_count,
            "available_daily_files": available_daily_files,
            "available_intraday_files": available_intraday_files,
            "market_file_count": len(market_files),
        }
        STATUS_FILE.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
        return counters

    def _sync_single(
        self,
        code: str,
        yahoo_symbol: str,
        counters: Dict[str, int],
        errors: List[Dict[str, str]],
    ) -> int:
        error_total = 0
        daily_path = parquet_path(code, "1d")
        intraday_path = parquet_path(code, "5m")

        if self.yahoo_blocked:
            counters["skipped_symbols"] += 1
            if daily_path.exists():
                counters["reused_daily_files"] += 1
            if intraday_path.exists():
                counters["reused_intraday_files"] += 1
            return 0

        try:
            daily = self.client.fetch_chart(yahoo_symbol, "1d", "2y")
            if not daily.empty:
                daily["code"] = code
                write_parquet(daily, daily_path)
                counters["daily_files"] += 1
        except Exception as exc:
            error_total += 1
            self._record_error(
                errors,
                {"code": code, "symbol": yahoo_symbol, "scope": "1d", "message": str(exc)},
            )
            if self._is_forbidden_error(exc):
                self.yahoo_forbidden_count += 1
                self._update_yahoo_blocked()
            if daily_path.exists():
                counters["reused_daily_files"] += 1

        if self.yahoo_blocked:
            counters["skipped_symbols"] += 1
            if intraday_path.exists():
                counters["reused_intraday_files"] += 1
            return error_total

        try:
            intraday = self.client.fetch_chart(yahoo_symbol, "5m", "60d")
            if not intraday.empty:
                intraday["code"] = code
                write_parquet(intraday, intraday_path)
                counters["intraday_files"] += 1
        except Exception as exc:
            error_total += 1
            self._record_error(
                errors,
                {"code": code, "symbol": yahoo_symbol, "scope": "5m", "message": str(exc)},
            )
            if self._is_forbidden_error(exc):
                self.yahoo_forbidden_count += 1
                self._update_yahoo_blocked()
            if intraday_path.exists():
                counters["reused_intraday_files"] += 1

        return error_total

    @staticmethod
    def _record_error(errors: List[Dict[str, str]], payload: Dict[str, str], limit: int = 120) -> None:
        if len(errors) < limit:
            errors.append(payload)

    @staticmethod
    def _is_forbidden_error(exc: Exception) -> bool:
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code == 403:
            return True
        text = str(exc).lower()
        return "403" in text and "forbidden" in text

    def _update_yahoo_blocked(self) -> None:
        if self.yahoo_forbidden_count >= 6:
            self.yahoo_blocked = True
