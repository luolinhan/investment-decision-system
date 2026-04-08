"""
百炼 Coding Plan 增强服务

用途:
1. 兼容 OpenAI 协议调用百炼模型生成每日情报摘要
2. 把情报结果写入本地 market_snapshots, 供盘前策略读取
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

from app.services.financial_news import FinancialNewsService
from app.services.investment_data import InvestmentDataService as RealtimeService
from app.services.investment_db_service import InvestmentDataService as DbService

DAILY_BRIEF_SNAPSHOT_KEY = "investment.intelligence.daily_brief.v1"
DAILY_BRIEF_TTL_SECONDS = 36 * 3600


class CodingPlanService:
    def __init__(
        self,
        realtime_service: Optional[RealtimeService] = None,
        db_service: Optional[DbService] = None,
        news_service: Optional[FinancialNewsService] = None,
    ):
        load_dotenv(".env.local", override=False)
        self.realtime = realtime_service or RealtimeService()
        self.db = db_service or DbService()
        self.news = news_service or FinancialNewsService()

        self.api_key = os.getenv("BAILIAN_API_KEY", "").strip()
        self.base_url = os.getenv("BAILIAN_BASE_URL", "https://coding.dashscope.aliyuncs.com/v1").rstrip("/")
        self.model = os.getenv("BAILIAN_MODEL", "qwen3-coder-plus").strip()
        self.timeout_seconds = int(os.getenv("BAILIAN_TIMEOUT_SECONDS", "25"))

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            if value in (None, "", "-", "--"):
                return default
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _extract_json_block(content: str) -> Dict[str, Any]:
        text = (content or "").strip()
        if not text:
            return {}

        fenced = re.search(r"```json\s*(\{.*\})\s*```", text, flags=re.DOTALL)
        if fenced:
            text = fenced.group(1).strip()

        try:
            return json.loads(text)
        except Exception:
            pass

        # fallback: extract first object block
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except Exception:
                return {}
        return {}

    def _collect_context(self) -> Dict[str, Any]:
        overview = self.realtime.get_market_overview(force_refresh=False)
        sentiment = overview.get("sentiment") or {}
        vix = ((overview.get("fear_greed") or {}).get("vix") or {})
        indices = overview.get("indices") or {}

        strategy = self.db.get_strategy_perf_overview(windows=[20, 60, 120])
        opportunity = self.db.get_opportunity_pool_overview(pool_code="all", limit=20)
        macro = self.db.get_north_money(25)
        news_payload = self.news.get_all_news()

        news_items: List[Dict[str, Any]] = []
        for key in ("bloomberg_markets", "bloomberg_economics", "bloomberg_tech"):
            rows = (news_payload.get("news") or {}).get(key) or []
            for item in rows[:3]:
                news_items.append(
                    {
                        "channel": key,
                        "title": str(item.get("title") or "")[:180],
                        "published": item.get("published"),
                    }
                )

        important_indices = {}
        for code in ("sh000300", "hsi", "ixic", "inx", "ftsea50"):
            item = indices.get(code) or {}
            important_indices[code] = {
                "name": item.get("name"),
                "change_pct": item.get("change_pct"),
                "date": item.get("date"),
            }

        return {
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "sentiment": {
                "date": sentiment.get("date"),
                "up_count": sentiment.get("up_count"),
                "down_count": sentiment.get("down_count"),
                "limit_up_count": sentiment.get("limit_up_count"),
                "limit_down_count": sentiment.get("limit_down_count"),
            },
            "vix": {
                "value": vix.get("value"),
                "change_pct": vix.get("change_pct"),
                "quote_time": vix.get("quote_time"),
            },
            "indices": important_indices,
            "north_money_25d": macro[-25:],
            "strategy_windows": strategy.get("windows", []),
            "strategy_setups": strategy.get("setups", [])[:8],
            "opportunity_summary": opportunity.get("summary", {}),
            "opportunity_top": [
                {
                    "code": item.get("display_code") or item.get("code"),
                    "name": item.get("name"),
                    "grade": item.get("grade"),
                    "total_score": item.get("total_score"),
                    "action": item.get("action_label"),
                }
                for item in (opportunity.get("leaderboard") or [])[:12]
            ],
            "news": news_items[:9],
        }

    def _llm_generate(self, context: Dict[str, Any]) -> Dict[str, Any]:
        if not self.api_key:
            return {
                "source_type": "fallback",
                "error": "missing_api_key",
                "summary": "未配置百炼API密钥，暂使用规则模式。",
                "execution_plan": [
                    "保持中性仓位，优先高覆盖A/B档。",
                    "开盘前再次确认外盘与北向方向。",
                ],
                "risk_watch": ["数据源异常时降低策略权重。"],
                "catalyst_watch": [],
                "confidence": 0.35,
            }

        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        prompt = (
            "你是中国股票市场的买方盘前策略分析师。"
            "请基于输入数据输出严格JSON，不要输出额外文本。"
            "JSON字段: summary(string), execution_plan(array[string]), risk_watch(array[string]), "
            "catalyst_watch(array[string]), confidence(number 0-1), regime_label(string), "
            "position_hint(string), top_watchlist(array[object{name,code,reason}])."
        )
        payload = {
            "model": self.model,
            "temperature": 0.2,
            "max_tokens": 1200,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(context, ensure_ascii=False)},
            ],
        }
        try:
            resp = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=self.timeout_seconds,
            )
            resp.raise_for_status()
            data = resp.json()
            content = (
                ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
                or ""
            )
            parsed = self._extract_json_block(content)
            if not parsed:
                return {
                    "source_type": "fallback",
                    "error": "invalid_llm_json",
                    "summary": str(content)[:400] or "LLM返回为空，已降级。",
                    "execution_plan": [],
                    "risk_watch": [],
                    "catalyst_watch": [],
                    "confidence": 0.3,
                }
            parsed["source_type"] = "llm"
            parsed["model"] = self.model
            return parsed
        except Exception as exc:
            return {
                "source_type": "fallback",
                "error": str(exc)[:300],
                "summary": "百炼接口调用失败，盘前策略改用规则引擎输出。",
                "execution_plan": ["保守仓位，等待确认信号。"],
                "risk_watch": ["外部接口异常导致信息不完整。"],
                "catalyst_watch": [],
                "confidence": 0.28,
            }

    def generate_daily_brief(self, force_refresh: bool = False, persist: bool = True) -> Dict[str, Any]:
        if not force_refresh:
            cached = self.get_latest_daily_brief(max_age_seconds=3 * 3600)
            if cached:
                return cached

        context = self._collect_context()
        llm_payload = self._llm_generate(context)
        result = {
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "context_as_of": context.get("generated_at"),
            "summary": llm_payload.get("summary"),
            "execution_plan": llm_payload.get("execution_plan") or [],
            "risk_watch": llm_payload.get("risk_watch") or [],
            "catalyst_watch": llm_payload.get("catalyst_watch") or [],
            "confidence": llm_payload.get("confidence"),
            "regime_label": llm_payload.get("regime_label"),
            "position_hint": llm_payload.get("position_hint"),
            "top_watchlist": llm_payload.get("top_watchlist") or [],
            "source_type": llm_payload.get("source_type"),
            "model": llm_payload.get("model") or self.model,
            "error": llm_payload.get("error"),
        }
        if persist:
            self.db.save_market_snapshot(
                DAILY_BRIEF_SNAPSHOT_KEY,
                result,
                DAILY_BRIEF_TTL_SECONDS,
                source=f"bailian:{result.get('source_type')}",
                notes="daily intelligence brief",
            )
        return result

    def get_latest_daily_brief(self, max_age_seconds: Optional[int] = None) -> Optional[Dict[str, Any]]:
        snapshot = self.db.get_market_snapshot(DAILY_BRIEF_SNAPSHOT_KEY, max_age_seconds=max_age_seconds)
        if not snapshot or not snapshot.get("payload"):
            return None
        payload = dict(snapshot["payload"])
        payload["storage"] = {
            "snapshot_key": DAILY_BRIEF_SNAPSHOT_KEY,
            "updated_at": snapshot.get("updated_at"),
            "age_seconds": snapshot.get("age_seconds"),
            "is_fresh": snapshot.get("is_fresh"),
            "source": snapshot.get("source"),
        }
        return payload
