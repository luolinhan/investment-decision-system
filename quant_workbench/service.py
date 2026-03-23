"""
量化工作台服务层
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from quant_workbench.config import INVESTMENT_DB_PATH, REPORTS_DB_PATH, STATUS_FILE
from quant_workbench.factors import analyze_structure, enrich_price_features, score_trend
from quant_workbench.storage import parquet_path, read_parquet
from quant_workbench.universe import BENCHMARKS, WATCHLIST, Instrument

POSITIVE_WORDS = [
    "买入", "增持", "推荐", "上调", "超配", "突破", "向上", "改善", "复苏", "拐点",
    "outperform", "overweight", "buy", "positive", "beat", "upside",
]
NEGATIVE_WORDS = [
    "减持", "卖出", "下调", "谨慎", "承压", "风险", "恶化", "回落", "失速",
    "underperform", "underweight", "sell", "negative", "miss", "downgrade",
]


class QuantWorkbenchService:
    def get_status(self) -> Dict[str, Any]:
        if STATUS_FILE.exists():
            return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
        return {"last_sync_at": None, "daily_files": 0, "intraday_files": 0}

    def get_market_regime(self) -> Dict[str, Any]:
        hsi = self._load_series("hsi", "1d")
        yinn = self._load_series("yinn", "1d")
        yang = self._load_series("yang", "1d")
        vix = self._load_series("vix", "1d")

        score = 0
        reasons: List[str] = []

        if not hsi.empty:
            latest = hsi.iloc[-1]
            hsi_close = latest.get("close")
            hsi_ma20 = latest.get("ma_20")
            hsi_ma60 = latest.get("ma_60")
            hsi_ret20 = latest.get("ret_20")
            if pd.notna(hsi_ma20) and pd.notna(hsi_ma60) and hsi_ma20 > hsi_ma60:
                score += 12
                reasons.append("恒生指数中期趋势转强")
            elif pd.notna(hsi_ma20) and pd.notna(hsi_ma60):
                score -= 6
                reasons.append("恒生指数中期均线仍偏弱")
            if pd.notna(hsi_close) and pd.notna(hsi_ma20) and hsi_close > hsi_ma20:
                score += 6
                reasons.append("恒生指数站上 20 日均线")
            elif pd.notna(hsi_close) and pd.notna(hsi_ma20):
                score -= 4
                reasons.append("恒生指数仍在 20 日均线下方")
            if pd.notna(hsi_ret20) and hsi_ret20 > 0:
                score += 4
                reasons.append("恒生指数近 20 日收益为正")
            elif pd.notna(hsi_ret20) and hsi_ret20 < -5:
                score -= 4
                reasons.append("恒生指数近 20 日回撤仍偏大")

        if not yinn.empty and pd.notna(yinn["ret_5"].iloc[-1]) and float(yinn["ret_5"].iloc[-1]) > 0:
            score += 4
            reasons.append("YINN 近 5 日偏强")
        if not yang.empty and pd.notna(yang["ret_5"].iloc[-1]) and float(yang["ret_5"].iloc[-1]) < 0:
            score += 4
            reasons.append("YANG 近 5 日走弱")
        if not yang.empty and pd.notna(yang["ret_5"].iloc[-1]) and float(yang["ret_5"].iloc[-1]) > 0:
            score -= 4
            reasons.append("YANG 近 5 日走强，空头压力仍在")
        if not vix.empty:
            current_vix = float(vix["close"].iloc[-1])
            if current_vix < 25:
                score += 4
                reasons.append("VIX 处于偏低区间，外部扰动较小")
            elif current_vix <= 32:
                reasons.append("VIX 处于中性区间")
            else:
                score -= 8
                reasons.append("VIX 偏高，需收紧仓位")

        if score >= 18:
            label = "risk_on"
        elif score >= 2:
            label = "neutral"
        else:
            label = "risk_off"

        return {"label": label, "score": score, "reasons": reasons}

    def list_opportunities(self) -> List[Dict[str, Any]]:
        regime = self.get_market_regime()
        results: List[Dict[str, Any]] = []

        for instrument in WATCHLIST:
            daily = self._load_series(instrument.code, "1d")
            intraday = self._load_series(instrument.code, "5m")
            if daily.empty:
                continue

            trend = score_trend(daily)
            structure = analyze_structure(intraday if not intraday.empty else daily.tail(120))
            fundamentals = self._load_fundamentals(instrument)
            sentiment = self._load_report_sentiment(instrument)

            tech_score = trend["score"] + structure["score"]
            tech_quality = round(((trend.get("confidence", 0.0) + structure.get("confidence", 0.0)) / 2) * 8, 1)
            fundamental_score, fundamental_reasons = self._score_fundamentals(fundamentals)
            sentiment_score, sentiment_reasons = self._score_sentiment(sentiment)
            macro_score = regime["score"]
            total_score = tech_score + fundamental_score + sentiment_score + macro_score + tech_quality

            risk_flags = []
            risk_flags.extend(regime.get("reasons", [])[:1] if regime.get("label") == "risk_off" else [])
            risk_flags.extend(trend.get("risk_flags", [])[:2])
            risk_flags.extend(structure.get("risk_flags", [])[:2])
            if sentiment.get("coverage", 0) < 2:
                risk_flags.append("研报标题覆盖不足，情绪信号较弱")
            if fundamentals.get("roe") is None:
                risk_flags.append("基本面覆盖不足")
            elif fundamentals.get("roe") < 8:
                risk_flags.append("ROE 偏低")

            grade = "C"
            if (
                total_score >= 74
                and tech_score >= 26
                and trend.get("score", 0) >= 16
                and structure.get("label") in {"second_buy", "center_break_attempt"}
                and structure.get("confidence", 0) >= 0.45
                and fundamental_score >= 10
                and sentiment.get("coverage", 0) >= 2
                and regime["label"] == "risk_on"
            ):
                grade = "A"
            elif total_score >= 60 and tech_score >= 18 and regime["label"] != "risk_off":
                grade = "B"

            latest = daily.iloc[-1]
            reasons = (
                trend["reasons"][:3]
                + structure["reasons"][:2]
                + fundamental_reasons[:2]
                + sentiment_reasons[:2]
            )
            if grade == "A":
                reasons = reasons[:6]
            elif grade == "B":
                reasons = reasons[:5]

            results.append(
                {
                    "code": instrument.code,
                    "name": instrument.name,
                    "market": instrument.market,
                    "category": instrument.category,
                    "grade": grade,
                    "score": round(total_score, 1),
                    "tech_score": round(tech_score, 1),
                    "fundamental_score": round(fundamental_score, 1),
                    "sentiment_score": round(sentiment_score, 1),
                    "macro_score": round(macro_score, 1),
                    "tech_quality": tech_quality,
                    "close": round(float(latest["close"]), 2),
                    "ret_5": round(float(latest["ret_5"]) if pd.notna(latest["ret_5"]) else 0, 2),
                    "ret_20": round(float(latest["ret_20"]) if pd.notna(latest["ret_20"]) else 0, 2),
                    "efi_13": round(float(latest["efi_13"]) if pd.notna(latest["efi_13"]) else 0, 2),
                    "structure": structure["label"],
                    "pe_ttm": fundamentals.get("pe_ttm"),
                    "pb": fundamentals.get("pb"),
                    "roe": fundamentals.get("roe"),
                    "sentiment": round(sentiment.get("score", 0), 2),
                    "coverage": sentiment.get("coverage", 0),
                    "reasons": reasons,
                    "risk_flags": risk_flags,
                }
            )

        grade_rank = {"A": 2, "B": 1, "C": 0}
        results.sort(key=lambda item: (grade_rank[item["grade"]], item["score"]), reverse=True)
        return results

    def get_stock_detail(self, code: str) -> Dict[str, Any]:
        instrument = next((item for item in WATCHLIST if item.code == code), None)
        if instrument is None:
            raise KeyError(code)

        daily = self._load_series(instrument.code, "1d")
        intraday = self._load_series(instrument.code, "5m")
        fundamentals = self._load_fundamentals(instrument)
        sentiment = self._load_report_sentiment(instrument, include_titles=True)
        structure = analyze_structure(intraday if not intraday.empty else daily.tail(120))
        trend = score_trend(daily)

        return {
            "instrument": {
                "code": instrument.code,
                "name": instrument.name,
                "market": instrument.market,
                "category": instrument.category,
            },
            "latest": self.list_opportunities_map().get(code),
            "fundamentals": fundamentals,
            "sentiment": sentiment,
            "structure": structure,
            "trend": trend,
            "daily": daily.tail(120)[["date", "close", "ma_20", "ma_60", "efi_13"]]
            .fillna(0)
            .to_dict(orient="records"),
        }

    def list_opportunities_map(self) -> Dict[str, Dict[str, Any]]:
        return {item["code"]: item for item in self.list_opportunities()}

    def _load_series(self, code: str, interval: str) -> pd.DataFrame:
        df = read_parquet(parquet_path(code, interval))
        if df.empty:
            return df
        return enrich_price_features(df)

    def _load_fundamentals(self, instrument: Instrument) -> Dict[str, Optional[float]]:
        if not INVESTMENT_DB_PATH.exists():
            return {"pe_ttm": None, "pb": None, "roe": None, "dividend_yield": None}

        conn = sqlite3.connect(INVESTMENT_DB_PATH)
        try:
            rows = conn.execute(
                """
                SELECT pe_ttm, pb, roe, dividend_yield, report_date
                FROM stock_financial
                WHERE code = ?
                ORDER BY report_date DESC
                LIMIT 8
                """,
                (instrument.finance_code,),
            ).fetchall()
            if not rows:
                return {"pe_ttm": None, "pb": None, "roe": None, "dividend_yield": None}

            latest_report_date = rows[0][4]
            pe_ttm = pb = roe = dividend_yield = None
            source_date = latest_report_date
            for row in rows:
                if pe_ttm is None and row[0] not in (None, ""):
                    pe_ttm = self._safe_round(row[0])
                if pb is None and row[1] not in (None, ""):
                    pb = self._safe_round(row[1])
                if roe is None and row[2] not in (None, ""):
                    roe = self._safe_round(row[2])
                    source_date = row[4]
                if dividend_yield is None and row[3] not in (None, ""):
                    dividend_yield = self._safe_round(row[3])
                if all(value is not None for value in (pe_ttm, pb, roe, dividend_yield)):
                    break

            return {
                "pe_ttm": pe_ttm,
                "pb": pb,
                "roe": roe,
                "dividend_yield": dividend_yield,
                "report_date": source_date,
                "snapshot_date": latest_report_date,
            }
        finally:
            conn.close()

    def _load_report_sentiment(self, instrument: Instrument, include_titles: bool = False) -> Dict[str, Any]:
        if not REPORTS_DB_PATH.exists():
            return {"score": 0.0, "coverage": 0, "titles": []}

        conn = sqlite3.connect(REPORTS_DB_PATH)
        conn.row_factory = sqlite3.Row
        titles: List[str] = []
        try:
            cutoff = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
            name_terms = [instrument.name, *instrument.aliases]
            clauses = ["stock_code = ?"]
            params: List[Any] = [instrument.report_code]
            for term in dict.fromkeys(name_terms):
                clauses.append("stock_name LIKE ?")
                params.append(f"%{term}%")
            rows = conn.execute(
                """
                SELECT title, institution, publish_date
                FROM reports
                WHERE ({conditions})
                  AND publish_date >= ?
                ORDER BY publish_date DESC, id DESC
                LIMIT 30
                """.format(conditions=" OR ".join(clauses)),
                (*params, cutoff),
            ).fetchall()

            score = 0
            coverage = len(rows)
            for row in rows:
                title = row["title"] or ""
                titles.append(title)
                score += self._score_title(title)

            normalized = 0.0
            if coverage:
                normalized = max(min(score / coverage, 1), -1)

            payload = {"score": normalized, "coverage": coverage}
            if include_titles:
                payload["titles"] = titles[:8]
            return payload
        finally:
            conn.close()

    def _score_fundamentals(self, data: Dict[str, Any]) -> tuple[float, List[str]]:
        score = 0.0
        reasons: List[str] = []

        roe = data.get("roe")
        pe_ttm = data.get("pe_ttm")
        pb = data.get("pb")
        dividend_yield = data.get("dividend_yield")

        if roe is not None and roe >= 15:
            score += 14
            reasons.append("ROE 高于 15%，盈利质量较强")
        elif roe is not None and roe >= 12:
            score += 10
            reasons.append("ROE 高于 12%，盈利质量较好")
        elif roe is not None and roe >= 8:
            score += 6
            reasons.append("ROE 保持在中高水平")
        elif roe is not None and roe < 8:
            score -= 4
            reasons.append("ROE 偏低，基本面弹性不足")

        if pe_ttm is not None and 0 < pe_ttm <= 20:
            score += 6
            reasons.append("PE-TTM 处于相对合理区间")
        elif pe_ttm is not None and 0 < pe_ttm <= 30:
            score += 4
            reasons.append("PE-TTM 处于可接受区间")
        elif pe_ttm is not None and pe_ttm > 45:
            score -= 3
            reasons.append("PE-TTM 偏高")

        if pb is not None and 0 < pb <= 3:
            score += 4
            reasons.append("PB 未显著透支")
        elif pb is not None and pb > 8:
            score -= 2
            reasons.append("PB 偏高")

        if dividend_yield is not None and dividend_yield >= 2:
            score += 2
            reasons.append("存在一定股息保护")

        return score, reasons

    def _score_sentiment(self, sentiment: Dict[str, Any]) -> tuple[float, List[str]]:
        coverage = sentiment.get("coverage", 0)
        raw_score = sentiment.get("score", 0)
        if coverage == 0:
            return 0.0, ["近期缺少可用研报标题样本"]

        reasons = []
        coverage_weight = min(coverage, 6) / 6
        scaled = round(raw_score * 6 * coverage_weight, 1)
        if raw_score > 0.25:
            reasons.append("近期研报标题情绪偏正面")
        elif raw_score < -0.25:
            reasons.append("近期研报标题情绪偏谨慎")
        else:
            reasons.append("近期研报标题情绪中性")
        reasons.append(f"近 60 天覆盖 {coverage} 篇")
        if coverage < 3:
            reasons.append("样本较少，情绪结论仅作参考")
        return scaled, reasons

    def _score_title(self, title: str) -> int:
        lowered = title.lower()
        score = 0
        for item in POSITIVE_WORDS:
            if item.lower() in lowered:
                score += 1
        for item in NEGATIVE_WORDS:
            if item.lower() in lowered:
                score -= 1
        return score

    @staticmethod
    def _safe_round(value: Any) -> Optional[float]:
        if value in (None, "", 0):
            return None if value in (None, "") else 0.0
        try:
            return round(float(value), 2)
        except (TypeError, ValueError):
            return None
