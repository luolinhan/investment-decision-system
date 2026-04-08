"""
盘前策略规划服务

目标:
1. 聚合前一交易日与最新分钟级行情快照, 生成可执行的盘前策略
2. 将结果写入本地 SQLite 快照, 供开盘前页面快速读取
3. 输出同花顺量化落地建议, 明确自动交易可行边界
"""
from __future__ import annotations

import concurrent.futures
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.services.coding_plan_service import CodingPlanService
from app.services.financial_news import FinancialNewsService
from app.services.investment_data import InvestmentDataService as RealtimeService
from app.services.investment_db_service import InvestmentDataService as DbService

PREOPEN_SNAPSHOT_KEY = "investment.strategy.preopen.v1"
PREOPEN_TTL_SECONDS = 24 * 3600


class StrategyPlanningService:
    def __init__(
        self,
        realtime_service: Optional[RealtimeService] = None,
        db_service: Optional[DbService] = None,
        news_service: Optional[FinancialNewsService] = None,
    ):
        self.realtime = realtime_service or RealtimeService()
        self.db = db_service or DbService()
        self.news = news_service or FinancialNewsService()
        self.intelligence = CodingPlanService(
            realtime_service=self.realtime,
            db_service=self.db,
            news_service=self.news,
        )

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            if value in (None, "", "-", "--"):
                return default
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _safe_ratio(numerator: float, denominator: float, default: float = 0.0) -> float:
        if denominator == 0:
            return default
        return numerator / denominator

    @staticmethod
    def _chanlun_proxy(item: Dict[str, Any]) -> str:
        trend = str(item.get("trend_signal") or "").lower()
        technical = float(item.get("technical_score") or 0.0)
        risk = float(item.get("risk_score") or 100.0)
        if trend == "bullish" and technical >= 60 and risk <= 45:
            return "中枢上移，优先三买候选（代理）"
        if trend == "neutral" and technical >= 50:
            return "中枢震荡，等待离开段确认（代理）"
        return "结构未确认，按中枢破坏处理（代理）"

    def _news_digest(self, per_feed: int = 2) -> Dict[str, Any]:
        feeds = {
            "bloomberg_markets": [],
            "bloomberg_economics": [],
            "bloomberg_tech": [],
        }
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(self.news.get_all_news)
                payload = future.result(timeout=4)
            for key in feeds.keys():
                rows = (payload.get("news") or {}).get(key) or []
                feeds[key] = [
                    {
                        "title": str(item.get("title") or "")[:180],
                        "published": item.get("published"),
                        "link": item.get("link"),
                    }
                    for item in rows[: max(1, int(per_feed or 2))]
                ]
        except concurrent.futures.TimeoutError:
            feeds["error"] = "news_timeout"
        except Exception as exc:
            feeds["error"] = str(exc)[:200]

        positive_words = ("rise", "rebound", "beat", "easing", "stimulus", "upgrade", "growth")
        negative_words = ("drop", "selloff", "downgrade", "risk", "war", "tariff", "inflation")
        pos = 0
        neg = 0
        for rows in feeds.values():
            if not isinstance(rows, list):
                continue
            for item in rows:
                title = str(item.get("title") or "").lower()
                if any(word in title for word in positive_words):
                    pos += 1
                if any(word in title for word in negative_words):
                    neg += 1
        signal = "neutral"
        if pos - neg >= 2:
            signal = "positive"
        elif neg - pos >= 2:
            signal = "negative"

        return {
            "feeds": feeds,
            "signal": signal,
            "positive_hits": pos,
            "negative_hits": neg,
        }

    def _international_context(self, indices: Dict[str, Any]) -> List[Dict[str, Any]]:
        watch = [
            ("dji", "道琼斯"),
            ("ixic", "纳斯达克"),
            ("inx", "标普500"),
            ("hsi", "恒生指数"),
            ("ftsea50", "富时中国A50"),
        ]
        result: List[Dict[str, Any]] = []
        for code, name in watch:
            item = indices.get(code) or {}
            result.append(
                {
                    "code": code,
                    "name": name,
                    "close": item.get("close"),
                    "change_pct": item.get("change_pct"),
                    "date": item.get("date"),
                    "source": item.get("source"),
                }
            )
        return result

    def _quant_plan(self, perf: Dict[str, Any]) -> Dict[str, Any]:
        setups = perf.get("setups") or []
        candidates = []
        for row in setups:
            sample = int(row.get("sample_size") or 0)
            win_rate = self._to_float(row.get("win_rate"))
            profit_factor = self._to_float(row.get("profit_factor"))
            enabled = sample >= 60 and win_rate >= 55 and profit_factor >= 1.0
            candidates.append(
                {
                    "setup_name": row.get("setup_name"),
                    "setup_label": row.get("setup_label") or row.get("setup_name"),
                    "sample_size": sample,
                    "win_rate": win_rate,
                    "profit_factor": profit_factor,
                    "enabled": enabled,
                }
            )

        enabled_count = sum(1 for x in candidates if x["enabled"])
        return {
            "auto_trade_ready": False,
            "reason": "同花顺交易端缺少稳定官方程序化交易API，需券商柜台或合规中间件授权后再自动下单。",
            "candidate_setups": candidates[:6],
            "enabled_setups": enabled_count,
            "execution_path": [
                "先做信号自动化与风控自动化，保留人工确认下单。",
                "如需自动下单，接入合规券商API或同花顺支持的受监管交易接口。",
                "先小资金灰度并记录实盘偏差，再放大仓位。",
            ],
        }

    def build_preopen_strategy(self, force_refresh: bool = False, persist: bool = True) -> Dict[str, Any]:
        now = datetime.now()
        trade_date = now.strftime("%Y-%m-%d")

        overview = self.realtime.get_market_overview(force_refresh=force_refresh)
        sentiment = overview.get("sentiment") or {}
        indices = overview.get("indices") or {}
        vix = ((overview.get("fear_greed") or {}).get("vix") or {})

        up_count = self._to_float(sentiment.get("up_count"))
        down_count = self._to_float(sentiment.get("down_count"))
        limit_up = self._to_float(sentiment.get("limit_up_count"))
        limit_down = self._to_float(sentiment.get("limit_down_count"))
        breadth_ratio = round(self._safe_ratio(up_count, max(1.0, down_count)), 2)
        limit_ratio = round(self._safe_ratio(limit_up, max(1.0, limit_down)), 2)

        north_rows = self.db.get_north_money(30)
        north_values = [self._to_float(item.get("total_inflow")) for item in north_rows if item.get("total_inflow") is not None]
        north_5d = round(sum(north_values[-5:]), 1) if north_values else 0.0
        north_20d_avg = round(sum(north_values[-20:]) / max(1, len(north_values[-20:])), 1) if north_values else 0.0

        vix_close = self._to_float(vix.get("value"), 20.0)
        if vix_close >= 24 or breadth_ratio < 0.9 or north_5d <= -80:
            regime = "risk_off"
            exposure = "10%-25%"
            core_action = "防守优先，仅跟踪A档且风控闸门全通过标的。"
        elif vix_close <= 16 and breadth_ratio >= 1.1 and north_5d >= 80:
            regime = "risk_on"
            exposure = "55%-75%"
            core_action = "进攻窗口，分批执行A/B档高覆盖标的。"
        else:
            regime = "neutral"
            exposure = "30%-50%"
            core_action = "中性仓位，优先高确定性与低回撤分型。"

        perf = self.db.get_strategy_perf_overview(windows=[20, 60, 120])
        windows = perf.get("windows") or []
        setups = perf.get("setups") or []
        recent_signals = perf.get("recent_signals") or []
        setup_focus = [
            row.get("setup_label") or row.get("setup_name")
            for row in setups
            if int(row.get("sample_size") or 0) >= 30 and self._to_float(row.get("win_rate")) >= 52
        ][:3]
        if not setup_focus:
            setup_focus = [
                row.get("setup_label") or row.get("setup_name")
                for row in setups[:2]
            ]

        opportunity = self.db.get_opportunity_pool_overview(pool_code="all", limit=60)
        pool_candidates = (opportunity.get("opportunities") or opportunity.get("leaderboard") or [])[:10]
        focus_universe = [
            {
                "code": item.get("display_code") or item.get("code"),
                "name": item.get("name"),
                "grade": item.get("grade"),
                "action": item.get("action_label"),
                "total_score": item.get("total_score"),
                "technical_score": item.get("technical_score"),
                "risk_score": item.get("risk_score"),
                "chanlun_proxy": self._chanlun_proxy(item),
            }
            for item in pool_candidates
        ]

        data_health = self.db.get_data_health_overview(
            snapshot_ttls={
                "investment.market_overview.v2": 180,
                "investment.watch_stocks.v2": 300,
                PREOPEN_SNAPSHOT_KEY: PREOPEN_TTL_SECONDS,
            }
        )
        storage = data_health.get("storage") or []
        fresh_count = sum(1 for item in storage if item.get("is_fresh"))
        storage_fresh_pct = round(self._safe_ratio(fresh_count * 100.0, max(1, len(storage))), 1)

        news_digest = self._news_digest(per_feed=2)
        international = self._international_context(indices)
        daily_intelligence = self.intelligence.get_latest_daily_brief(max_age_seconds=16 * 3600)
        if not daily_intelligence:
            daily_intelligence = self.intelligence.generate_daily_brief(
                force_refresh=False,
                persist=True,
            )

        warning_flags: List[str] = []
        if storage_fresh_pct < 80:
            warning_flags.append("数据新鲜度不足，降低仓位并减少追单。")
        if windows:
            w120 = next((item for item in windows if int(item.get("window_size") or 0) == 120), {})
            if self._to_float(w120.get("win_rate")) < 50:
                warning_flags.append("120日窗口胜率偏弱，优先防守与复盘。")
        if news_digest.get("signal") == "negative":
            warning_flags.append("国际新闻信号偏负面，盘中优先控制回撤。")

        result = {
            "generated_at": now.replace(microsecond=0).isoformat(),
            "trade_date": trade_date,
            "reference_as_of": perf.get("as_of_date"),
            "regime": regime,
            "target_exposure": exposure,
            "core_action": core_action,
            "market_context": {
                "breadth_ratio": breadth_ratio,
                "limit_ratio": limit_ratio,
                "north_money_5d": north_5d,
                "north_money_20d_avg": north_20d_avg,
                "vix_close": round(vix_close, 2),
            },
            "international_context": international,
            "news_digest": news_digest,
            "daily_intelligence": {
                "summary": daily_intelligence.get("summary"),
                "execution_plan": daily_intelligence.get("execution_plan") or [],
                "risk_watch": daily_intelligence.get("risk_watch") or [],
                "confidence": daily_intelligence.get("confidence"),
                "source_type": daily_intelligence.get("source_type"),
                "model": daily_intelligence.get("model"),
                "generated_at": daily_intelligence.get("generated_at"),
            },
            "strategy_windows": windows,
            "strategy_setups": setups[:8],
            "recent_signals": recent_signals[:12],
            "setup_focus": setup_focus,
            "focus_universe": focus_universe,
            "risk_controls": [
                "单标的仓位不超过总资金的8%。",
                "若盘中VIX走高且北向转负，执行降仓。",
                "未通过闸门（风险>45或覆盖<55）的标的不入池。",
            ],
            "warning_flags": warning_flags,
            "data_freshness": {
                "storage_fresh_pct": storage_fresh_pct,
                "fresh_count": fresh_count,
                "total_storage": len(storage),
            },
            "quant_plan": self._quant_plan(perf),
        }

        if persist:
            self.db.save_market_snapshot(
                PREOPEN_SNAPSHOT_KEY,
                result,
                PREOPEN_TTL_SECONDS,
                source="strategy_planner",
                notes="盘前策略自动生成",
            )
        return result

    def get_latest_preopen_strategy(self, max_age_seconds: Optional[int] = None) -> Optional[Dict[str, Any]]:
        snapshot = self.db.get_market_snapshot(PREOPEN_SNAPSHOT_KEY, max_age_seconds=max_age_seconds)
        if not snapshot or not snapshot.get("payload"):
            return None
        payload = dict(snapshot["payload"])
        payload["storage"] = {
            "snapshot_key": PREOPEN_SNAPSHOT_KEY,
            "updated_at": snapshot.get("updated_at"),
            "age_seconds": snapshot.get("age_seconds"),
            "is_fresh": snapshot.get("is_fresh"),
            "source": snapshot.get("source"),
        }
        return payload
