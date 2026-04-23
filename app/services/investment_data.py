"""
投资决策数据服务

目标:
1. 当前行情默认走实时源, 避免受日更库表影响
2. 使用备用源做轻量交叉校验, 发现异常时回退
3. 为前端提供 5 分钟级缓存, 和页面刷新节奏保持一致
"""
import os
import re
import threading
import time
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from email.utils import parsedate_to_datetime
from io import StringIO
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# 清除代理环境变量（东方财富直连不需要代理）
for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
    os.environ.pop(_k, None)

import akshare as ak

DEFAULT_WATCH_LIST = {
    "a_stocks": {
        # 科技 / 半导体
        "中芯国际": "sh688981",
        "立讯精密": "sz002475",
        "韦尔股份": "sh603501",
        "澜起科技": "sh688008",
        "兆易创新": "sh603986",
        "中科创达": "sz300496",
        "科大讯飞": "sz002230",
        "金山办公": "sh688111",
        # 新能源 / 光伏
        "宁德时代": "sz300750",
        "隆基绿能": "sh601012",
        "晶澳科技": "sz002459",
        "通威股份": "sh600438",
        "锦浪科技": "sz300763",
        "亿纬锂能": "sz300014",
        "赣锋锂业": "sz002460",
        # 消费 / 白酒
        "贵州茅台": "sh600519",
        "五粮液": "sz000858",
        "泸州老窖": "sz000568",
        "山西汾酒": "sh600809",
        "海天味业": "sh603288",
        "中国中免": "sh601888",
        # 医药 / CXO
        "百济神州": "sh688235",
        "药明康德": "sh603259",
        "复星医药": "sh600196",
        "迈瑞医疗": "sz300760",
        "恒瑞医药": "sh600276",
        # 金融
        "招商银行": "sh600036",
        "中国平安": "sh601318",
        "中信证券": "sh600030",
        # 制造 / 家电
        "格力电器": "sz000651",
        "美的集团": "sz000333",
        "紫金矿业": "sh601899",
        "顺丰控股": "sz002352",
    },
    "hk_stocks": {
        # 恒生科技
        "阿里巴巴": "hk09988",
        "腾讯": "hk00700",
        "美团": "hk03690",
        "小米": "hk01810",
        "快手": "hk01024",
        "京东": "hk09618",
        "百度": "hk09888",
        # 汽车
        "比亚迪股份": "hk01211",
        "理想汽车": "hk02015",
        "小鹏汽车": "hk09868",
        "蔚来": "hk09866",
        # 医药
        "百济神州": "hk06160",
        "药明生物": "hk02269",
        "信达生物": "hk01801",
        # 金融 / 能源 / 消费
        "中国平安": "hk02318",
        "中海油": "hk00883",
        "中芯国际": "hk00981",
        "中国移动": "hk00941",
        "友邦保险": "hk01299",
        "联想集团": "hk00992",
    },
    "us_stocks": {
        "阿里巴巴": "usBABA",
        "腾讯ADR": "usTCEHY",
        "拼多多": "usPDD",
        "京东ADR": "usJD",
        "网易": "usNTES",
    },
}

INDEX_CODES = {
    "sh000001": {"name": "上证指数", "primary": "sh000001", "secondary": "s_sh000001"},
    "sz399001": {"name": "深证成指", "primary": "sz399001", "secondary": "s_sz399001"},
    "sz399006": {"name": "创业板指", "primary": "sz399006", "secondary": "s_sz399006"},
    "sh000300": {"name": "沪深300", "primary": "sh000300", "secondary": "s_sh000300"},
    "sh000016": {"name": "上证50", "primary": "sh000016", "secondary": "s_sh000016"},
    "sh000905": {"name": "中证500", "primary": "sh000905", "secondary": "s_sh000905"},
    "sh000852": {"name": "中证1000", "primary": "sh000852", "secondary": "s_sh000852"},
    "sz399005": {"name": "中小板指", "primary": "sz399005", "secondary": "s_sz399005"},
    "sz399324": {"name": "红利指数", "primary": "sz399324", "secondary": "s_sz399324"},
    "hsi": {"name": "恒生指数", "primary": "hkHSI", "secondary": "rt_hkHSI"},
    "hstech": {"name": "恒生科技", "primary": "hkHSTECH", "secondary": "rt_hkHSTECH"},
    "ftsea50": {"name": "富时中国A50", "primary": "usFXI", "secondary": "gb_fxi"},
    "yang": {"name": "富时中国三倍做空", "primary": "usYANG", "secondary": "gb_yang"},
    "dji": {"name": "道琼斯", "primary": "usDJI", "secondary": "gb_dji"},
    "ixic": {"name": "纳斯达克", "primary": "usIXIC", "secondary": "gb_ixic"},
    "inx": {"name": "标普500", "primary": "usSPX", "secondary": "gb_spx", "yahoo": "^GSPC"},
}

OVERVIEW_CACHE_TTL_SECONDS = 60
WATCH_CACHE_TTL_SECONDS = 120
GLOBAL_RISK_CACHE_TTL_SECONDS = 600
QUOTE_VALIDATION_TOLERANCE = 0.005
VIX_VALIDATION_TOLERANCE = 0.10
OVERVIEW_SNAPSHOT_KEY = "investment.market_overview.v2"
WATCH_SNAPSHOT_KEY = "investment.watch_stocks.v2"


class InvestmentDataService:
    """投资决策数据服务"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                "Referer": "https://gu.qq.com/",
            }
        )

        # 直连会话用于 Yahoo / HKAB 这类不需要本地代理的站点.
        self.direct_session = requests.Session()
        self.direct_session.trust_env = False
        self.direct_session.headers.update({"User-Agent": self.session.headers["User-Agent"]})

        self._overview_cache: Optional[Dict[str, Any]] = None
        self._overview_cache_at: float = 0.0
        self._watch_cache: Optional[Dict[str, Any]] = None
        self._watch_cache_at: float = 0.0
        self._global_risk_cache: Optional[Dict[str, Any]] = None
        self._global_risk_cache_at: float = 0.0
        self._overview_refreshing = False
        self._watch_refreshing = False
        self._db_service = None

    def _get_db_service(self):
        if self._db_service is None:
            from app.services.investment_db_service import InvestmentDataService as DbService

            self._db_service = DbService()
        return self._db_service

    def _refresh_overview_async(self) -> None:
        if self._overview_refreshing:
            return
        self._overview_refreshing = True

        def _worker() -> None:
            try:
                self.get_market_overview(force_refresh=True)
            except Exception as exc:
                print(f"异步刷新市场概览失败: {exc}")
            finally:
                self._overview_refreshing = False

        threading.Thread(target=_worker, daemon=True).start()

    def _refresh_watch_async(self) -> None:
        if self._watch_refreshing:
            return
        self._watch_refreshing = True

        def _worker() -> None:
            try:
                self.get_watch_stocks(force_refresh=True)
            except Exception as exc:
                print(f"异步刷新自选池失败: {exc}")
            finally:
                self._watch_refreshing = False

        threading.Thread(target=_worker, daemon=True).start()

    def _snapshot_meta(
        self,
        snapshot_key: str,
        mode: str,
        source: str,
        updated_at: Optional[str] = None,
        age_seconds: Optional[int] = None,
        is_fresh: Optional[bool] = None,
        fetch_latency_ms: Optional[int] = None,
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "mode": mode,
            "db_path": "data/investment.db",
            "snapshot_key": snapshot_key,
            "source": source,
            "updated_at": updated_at,
            "age_seconds": age_seconds,
            "is_fresh": is_fresh,
            "fetch_latency_ms": fetch_latency_ms,
            "notes": notes,
        }

    def _attach_storage_meta(
        self,
        payload: Dict[str, Any],
        snapshot_key: str,
        mode: str,
        source: str,
        updated_at: Optional[str] = None,
        age_seconds: Optional[int] = None,
        is_fresh: Optional[bool] = None,
        fetch_latency_ms: Optional[int] = None,
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        enriched = dict(payload)
        enriched["storage"] = self._snapshot_meta(
            snapshot_key=snapshot_key,
            mode=mode,
            source=source,
            updated_at=updated_at,
            age_seconds=age_seconds,
            is_fresh=is_fresh,
            fetch_latency_ms=fetch_latency_ms,
            notes=notes,
        )
        return enriched

    def _load_watch_list_rows(self) -> List[Dict[str, Any]]:
        try:
            rows = self._get_db_service().get_watch_list()
            if rows:
                return rows
        except Exception as exc:
            print(f"watch_list读取失败，改用默认列表: {exc}")

        fallback: List[Dict[str, Any]] = []
        for group, mapping in DEFAULT_WATCH_LIST.items():
            market = "A" if group == "a_stocks" else "HK" if group == "hk_stocks" else "US"
            for name, code in mapping.items():
                fallback.append(
                    {
                        "code": code,
                        "name": name,
                        "market": market,
                        "category": "默认池",
                        "weight": 1.0,
                        "notes": "fallback",
                    }
                )
        return fallback

    def _watch_group_key(self, row: Dict[str, Any]) -> str:
        code = (row.get("code") or "").lower()
        market = (row.get("market") or "").upper()
        if market in {"A", "CN"} or code.startswith(("sh", "sz", "bj")):
            return "a_stocks"
        if market == "HK" or code.startswith("hk"):
            return "hk_stocks"
        if market == "US" or code.startswith("us"):
            return "us_stocks"
        return "other_stocks"

    def _build_watch_entry(
        self,
        row: Dict[str, Any],
        quote: Optional[Dict[str, Any]],
        fundamentals: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        quote = quote or {}
        fundamentals = fundamentals or {}
        code = row.get("code")

        return {
            "code": code,
            "display_code": code[2:] if isinstance(code, str) and code.lower().startswith("us") else code,
            "name": row.get("name"),
            "market": row.get("market"),
            "category": row.get("category"),
            "weight": row.get("weight"),
            "notes": row.get("notes"),
            "price": quote.get("price"),
            "prev_close": quote.get("prev_close"),
            "open": quote.get("open"),
            "high": quote.get("high"),
            "low": quote.get("low"),
            "change": quote.get("change"),
            "change_pct": quote.get("change_pct"),
            "volume": quote.get("volume"),
            "quote_time": quote.get("quote_time"),
            "quote_source": quote.get("source"),
            "validated": quote.get("validated"),
            "validated_with": quote.get("validated_with"),
            "validation_diff_pct": quote.get("validation_diff_pct"),
            "report_date": fundamentals.get("report_date"),
            "pe_ttm": fundamentals.get("pe_ttm"),
            "pb": fundamentals.get("pb"),
            "roe": fundamentals.get("roe"),
            "gross_margin": fundamentals.get("gross_margin"),
            "revenue_yoy": fundamentals.get("revenue_yoy"),
            "net_profit_yoy": fundamentals.get("net_profit_yoy"),
            "dividend_yield": fundamentals.get("dividend_yield"),
        }

    def _build_watch_summary(self, result: Dict[str, Any]) -> Dict[str, Any]:
        all_items = (
            result.get("a_stocks", [])
            + result.get("hk_stocks", [])
            + result.get("us_stocks", [])
        )
        priced_count = sum(1 for item in all_items if item.get("price") is not None)
        total = len(all_items)
        categories = sorted({item.get("category") for item in all_items if item.get("category")})
        return {
            "total": total,
            "priced_count": priced_count,
            "coverage_pct": round((priced_count / total) * 100, 1) if total else 0.0,
            "markets": {
                "A": len(result.get("a_stocks", [])),
                "HK": len(result.get("hk_stocks", [])),
                "US": len(result.get("us_stocks", [])),
            },
            "categories": categories,
        }

    def _cache_is_fresh(self, cached_at: float, ttl_seconds: int) -> bool:
        return (time.time() - cached_at) < ttl_seconds

    def _safe_float(self, value: Any) -> Optional[float]:
        if value in (None, "", "-", "--"):
            return None
        try:
            return float(str(value).replace(",", "").strip())
        except (TypeError, ValueError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        parsed = self._safe_float(value)
        if parsed is None:
            return None
        return int(parsed)

    def _compute_change_pct(self, price: Optional[float], prev_close: Optional[float]) -> Optional[float]:
        if price is None or prev_close in (None, 0):
            return None
        return round((price - prev_close) / prev_close * 100, 2)

    def _parse_quote_time(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        text = value.strip()
        for fmt in (
            "%Y%m%d%H%M%S",
            "%Y%m%d%H%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
        ):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    def _relative_diff(self, a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is None or b in (None, 0):
            return None
        return abs(a - b) / abs(b)

    def _get_tencent_quote(self, codes: List[str]) -> str:
        url = "https://qt.gtimg.cn/q=" + ",".join(codes)
        try:
            resp = self.session.get(url, timeout=10)
            resp.encoding = "gbk"
            return resp.text
        except Exception:
            return ""

    def _get_sina_quote(self, codes: List[str]) -> str:
        url = "http://hq.sinajs.cn/list=" + ",".join(codes)
        headers = {
            "Referer": "http://finance.sina.com.cn",
            "User-Agent": self.session.headers["User-Agent"],
        }
        try:
            resp = self.session.get(url, headers=headers, timeout=10)
            resp.encoding = "gbk"
            return resp.text
        except Exception:
            return ""

    def _extract_tencent_payload(self, text: str, code: str) -> Optional[str]:
        match = re.search(rf'v_{re.escape(code)}="(.*?)";', text)
        if not match:
            return None
        payload = match.group(1)
        return payload or None

    def _extract_sina_payload(self, text: str, code: str) -> Optional[str]:
        match = re.search(rf'var hq_str_{re.escape(code)}="(.*?)";', text)
        if not match:
            return None
        payload = match.group(1)
        return payload or None

    def _parse_tencent_quote(self, text: str, code: str) -> Optional[Dict[str, Any]]:
        payload = self._extract_tencent_payload(text, code)
        if not payload:
            return None

        parts = payload.split("~")
        if len(parts) < 35:
            return None

        price = self._safe_float(parts[3])
        prev_close = self._safe_float(parts[4])

        return {
            "name": parts[1],
            "code": parts[2],
            "price": price,
            "prev_close": prev_close,
            "open": self._safe_float(parts[5]),
            "change": self._safe_float(parts[31] if len(parts) > 31 else None),
            "change_pct": self._safe_float(parts[32] if len(parts) > 32 else None),
            "high": self._safe_float(parts[33] if len(parts) > 33 else None),
            "low": self._safe_float(parts[34] if len(parts) > 34 else None),
            "volume": self._safe_int(parts[6] if len(parts) > 6 else None),
            "quote_time": parts[30] if len(parts) > 30 else None,
            "source": "tencent",
        }

    def _parse_sina_a_quote(self, text: str, code: str) -> Optional[Dict[str, Any]]:
        payload = self._extract_sina_payload(text, code)
        if not payload:
            return None

        parts = payload.split(",")
        if len(parts) < 6:
            return None

        price = self._safe_float(parts[3])
        prev_close = self._safe_float(parts[2])
        quote_time = None
        if len(parts) > 31:
            quote_time = f"{parts[30]} {parts[31]}".strip()

        return {
            "name": parts[0],
            "code": code.replace("sz", "").replace("sh", ""),
            "price": price,
            "prev_close": prev_close,
            "open": self._safe_float(parts[1]),
            "high": self._safe_float(parts[4]),
            "low": self._safe_float(parts[5]),
            "volume": self._safe_int(parts[8] if len(parts) > 8 else None),
            "change": None if price is None or prev_close is None else round(price - prev_close, 2),
            "change_pct": self._compute_change_pct(price, prev_close),
            "quote_time": quote_time,
            "source": "sina",
        }

    def _parse_sina_hk_quote(self, text: str, code: str) -> Optional[Dict[str, Any]]:
        payload = self._extract_sina_payload(text, code)
        if not payload:
            return None

        parts = payload.split(",")
        if len(parts) < 9:
            return None

        quote_time = None
        if len(parts) > 18:
            quote_time = f"{parts[17]} {parts[18]}".strip()

        return {
            "name": parts[1] if len(parts) > 1 and parts[1] else parts[0],
            "code": code.replace("hk", ""),
            "price": self._safe_float(parts[6]),
            "prev_close": self._safe_float(parts[3]),
            "open": self._safe_float(parts[2]),
            "high": self._safe_float(parts[4]),
            "low": self._safe_float(parts[5]),
            "change": self._safe_float(parts[7]),
            "change_pct": self._safe_float(parts[8]),
            "quote_time": quote_time,
            "source": "sina",
        }

    def _parse_sina_simple_index(self, text: str, code: str) -> Optional[Dict[str, Any]]:
        payload = self._extract_sina_payload(text, code)
        if not payload:
            return None

        parts = payload.split(",")
        if len(parts) < 4:
            return None

        return {
            "name": parts[0],
            "price": self._safe_float(parts[1]),
            "change": self._safe_float(parts[2]),
            "change_pct": self._safe_float(parts[3]),
            "quote_time": None,
            "source": "sina",
        }

    def _parse_sina_hk_index(self, text: str, code: str) -> Optional[Dict[str, Any]]:
        payload = self._extract_sina_payload(text, code)
        if not payload:
            return None

        parts = payload.split(",")
        if len(parts) < 9:
            return None

        quote_time = None
        if len(parts) > 18:
            quote_time = f"{parts[17]} {parts[18]}".strip()

        return {
            "name": parts[1] if len(parts) > 1 else parts[0],
            "price": self._safe_float(parts[6]),
            "change": self._safe_float(parts[7]),
            "change_pct": self._safe_float(parts[8]),
            "quote_time": quote_time,
            "source": "sina",
        }

    def _parse_sina_us_index(self, text: str, code: str) -> Optional[Dict[str, Any]]:
        payload = self._extract_sina_payload(text, code)
        if not payload:
            return None

        parts = payload.split(",")
        if len(parts) < 5:
            return None

        return {
            "name": parts[0],
            "price": self._safe_float(parts[1]),
            "change": self._safe_float(parts[4]),
            "change_pct": self._safe_float(parts[2]),
            "quote_time": parts[3],
            "source": "sina",
        }

    def _merge_with_secondary(
        self,
        primary: Optional[Dict[str, Any]],
        secondary: Optional[Dict[str, Any]],
        tolerance: float = QUOTE_VALIDATION_TOLERANCE,
        always_prefer_secondary: bool = False,
    ) -> Optional[Dict[str, Any]]:
        if primary is None:
            return secondary
        if secondary is None:
            primary["validated"] = False
            primary["validated_with"] = None
            primary["validation_diff_pct"] = None
            return primary

        diff_ratio = self._relative_diff(primary.get("price"), secondary.get("price"))
        primary["validated_with"] = secondary.get("source")
        primary["validation_diff_pct"] = None if diff_ratio is None else round(diff_ratio * 100, 3)
        primary["validated"] = diff_ratio is None or diff_ratio <= tolerance

        if always_prefer_secondary or (diff_ratio is not None and diff_ratio > tolerance):
            primary_ts = self._parse_quote_time(primary.get("quote_time"))
            secondary_ts = self._parse_quote_time(secondary.get("quote_time"))
            if always_prefer_secondary or (secondary_ts and (not primary_ts or secondary_ts >= primary_ts)):
                merged = dict(secondary)
                merged["validated_with"] = primary.get("source")
                merged["validation_diff_pct"] = primary["validation_diff_pct"]
                merged["validated"] = diff_ratio is None or diff_ratio <= tolerance
                return merged

        return primary

    def _fetch_realtime_indices(self) -> Dict[str, Dict[str, Any]]:
        primary_text = self._get_tencent_quote([item["primary"] for item in INDEX_CODES.values()])
        secondary_text = self._get_sina_quote([item["secondary"] for item in INDEX_CODES.values()])

        result: Dict[str, Dict[str, Any]] = {}
        for code, meta in INDEX_CODES.items():
            primary = self._parse_tencent_quote(primary_text, meta["primary"])
            secondary_code = meta["secondary"]

            if secondary_code.startswith("s_"):
                secondary = self._parse_sina_simple_index(secondary_text, secondary_code)
            elif secondary_code.startswith("rt_hk"):
                secondary = self._parse_sina_hk_index(secondary_text, secondary_code)
            else:
                secondary = self._parse_sina_us_index(secondary_text, secondary_code)

            merged = self._merge_with_secondary(primary, secondary)
            if (not merged or merged.get("price") is None) and meta.get("yahoo"):
                yahoo_quote = self._fetch_yahoo_chart_quote(meta["yahoo"], meta["name"])
                if yahoo_quote and yahoo_quote.get("value") is not None:
                    merged = {
                        "name": meta["name"],
                        "price": yahoo_quote.get("value"),
                        "change_pct": yahoo_quote.get("change_pct"),
                        "quote_time": yahoo_quote.get("quote_time"),
                        "source": yahoo_quote.get("source"),
                        "validated": None,
                        "validated_with": None,
                        "validation_diff_pct": None,
                    }
            if not merged or merged.get("price") is None:
                continue

            quote_time = merged.get("quote_time")
            parsed_quote_time = self._parse_quote_time(quote_time)
            quote_date = parsed_quote_time.strftime("%Y-%m-%d") if parsed_quote_time else datetime.now().strftime("%Y-%m-%d")

            result[code] = {
                "name": meta["name"],
                "code": code,
                "close": merged.get("price"),
                "change_pct": merged.get("change_pct"),
                "date": quote_date,
                "quote_time": quote_time,
                "source": merged.get("source"),
                "validated": merged.get("validated"),
                "validated_with": merged.get("validated_with"),
                "validation_diff_pct": merged.get("validation_diff_pct"),
            }

        return result

    def _fetch_watch_stocks_uncached(self) -> Dict[str, Any]:
        result = {
            "a_stocks": [],
            "hk_stocks": [],
            "us_stocks": [],
            "update_time": datetime.now().isoformat(),
        }

        watch_rows = self._load_watch_list_rows()
        fundamentals_map = {
            item.get("code"): item
            for item in self._get_db_service().get_watch_stocks_fundamentals()
        }

        grouped_rows: Dict[str, List[Dict[str, Any]]] = {
            "a_stocks": [],
            "hk_stocks": [],
            "us_stocks": [],
        }
        for row in watch_rows:
            group_key = self._watch_group_key(row)
            if group_key in grouped_rows:
                grouped_rows[group_key].append(row)

        a_codes = [row["code"] for row in grouped_rows["a_stocks"]]
        hk_codes = [row["code"] for row in grouped_rows["hk_stocks"]]
        quote_codes = a_codes + hk_codes

        primary_text = self._get_tencent_quote(quote_codes) if quote_codes else ""
        secondary_text = self._get_sina_quote(quote_codes) if quote_codes else ""

        for row in grouped_rows["a_stocks"]:
            code = row["code"]
            primary = self._parse_tencent_quote(primary_text, code)
            secondary = self._parse_sina_a_quote(secondary_text, code)
            merged = self._merge_with_secondary(primary, secondary)
            result["a_stocks"].append(
                self._build_watch_entry(row, merged, fundamentals_map.get(code))
            )

        for row in grouped_rows["hk_stocks"]:
            code = row["code"]
            primary = self._parse_tencent_quote(primary_text, code)
            secondary = self._parse_sina_hk_quote(secondary_text, code)
            merged = self._merge_with_secondary(primary, secondary)
            result["hk_stocks"].append(
                self._build_watch_entry(row, merged, fundamentals_map.get(code))
            )

        for row in grouped_rows["us_stocks"]:
            code = row["code"]
            symbol = code[2:] if code.lower().startswith("us") else code
            yahoo_quote = self._fetch_yahoo_chart_quote(symbol, row.get("name") or symbol)
            quote = None
            if yahoo_quote:
                quote = {
                    "price": yahoo_quote.get("value"),
                    "prev_close": yahoo_quote.get("prev_close"),
                    "change_pct": yahoo_quote.get("change_pct"),
                    "quote_time": yahoo_quote.get("quote_time"),
                    "source": yahoo_quote.get("source"),
                }
                if quote["price"] is not None and quote["prev_close"] is not None:
                    quote["change"] = round(quote["price"] - quote["prev_close"], 2)
            result["us_stocks"].append(
                self._build_watch_entry(row, quote, fundamentals_map.get(code))
            )

        result["summary"] = self._build_watch_summary(result)

        return result

    def _fetch_yahoo_chart_quote(self, symbol: str, name: str) -> Optional[Dict[str, Any]]:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {"range": "1d", "interval": "1m"}

        last_error = None
        for session in (self.direct_session, self.session):
            try:
                resp = session.get(url, params=params, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                result = data["chart"]["result"][0]
                meta = result.get("meta", {})
                timestamps = result.get("timestamp") or []
                closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])

                price = self._safe_float(meta.get("regularMarketPrice"))
                if price is None:
                    valid_closes = [self._safe_float(item) for item in closes if item is not None]
                    valid_closes = [item for item in valid_closes if item is not None]
                    price = valid_closes[-1] if valid_closes else None

                prev_close = self._safe_float(meta.get("chartPreviousClose"))
                market_time = meta.get("regularMarketTime") or (timestamps[-1] if timestamps else None)
                quote_time = None
                if market_time:
                    quote_time = datetime.fromtimestamp(int(market_time)).strftime("%Y-%m-%d %H:%M:%S")

                if price is None:
                    return None

                return {
                    "name": name,
                    "value": price,
                    "prev_close": prev_close,
                    "change_pct": self._compute_change_pct(price, prev_close),
                    "quote_time": quote_time,
                    "source": "yahoo",
                }
            except Exception as exc:
                last_error = exc

        if last_error:
            print(f"{name}获取失败: {last_error}")
        return None

    def _fetch_vix_yahoo(self) -> Optional[Dict[str, Any]]:
        return self._fetch_yahoo_chart_quote("%5EVIX", "VIX恐慌指数")

    def _fetch_fred_series(self, series_id: str, days: int = 180) -> List[Dict[str, Any]]:
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
        last_error = None

        for session in (self.session, self.direct_session):
            try:
                resp = session.get(url, timeout=20)
                resp.raise_for_status()
                df = pd.read_csv(StringIO(resp.text))
                if df is None or df.empty or len(df.columns) < 2:
                    return []

                date_col, value_col = df.columns[:2]
                df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
                df = df.dropna(subset=[value_col]).tail(days)
                return [
                    {"date": str(row[date_col])[:10], "value": round(float(row[value_col]), 2)}
                    for _, row in df.iterrows()
                ]
            except Exception as exc:
                last_error = exc

        if last_error:
            print(f"FRED {series_id} 获取失败: {last_error}")
        return []

    def _build_series_summary(self, history: List[Dict[str, Any]], source: str, unit: str = "") -> Dict[str, Any]:
        if not history:
            return {"history": [], "source": source, "unit": unit}

        latest = history[-1]
        previous = history[-2] if len(history) > 1 else None
        latest_value = latest.get("value")
        previous_value = previous.get("value") if previous else None
        delta = None
        delta_pct = None
        if latest_value is not None and previous_value is not None:
            delta = round(latest_value - previous_value, 2)
            delta_pct = self._compute_change_pct(latest_value, previous_value)

        return {
            "latest": latest_value,
            "previous": previous_value,
            "delta": delta,
            "delta_pct": delta_pct,
            "as_of": latest.get("date"),
            "history": history,
            "source": source,
            "unit": unit,
        }

    def _classify_us10y(self, value: Optional[float]) -> Dict[str, str]:
        if value is None:
            return {}
        if value >= 4.5:
            return {
                "regime": "高位偏紧",
                "commentary": "长端利率处于高位，成长和高估值资产更容易承压。",
            }
        if value >= 4.0:
            return {
                "regime": "中性偏紧",
                "commentary": "利率仍在约束估值扩张，市场更依赖盈利兑现和政策边际。",
            }
        if value >= 3.5:
            return {
                "regime": "中性",
                "commentary": "利率环境相对均衡，风险偏好更多取决于盈利与流动性预期。",
            }
        return {
            "regime": "偏宽松",
            "commentary": "长端利率较低，成长资产的估值压力相对缓和。",
        }

    def _classify_vix(self, value: Optional[float]) -> Dict[str, str]:
        if value is None:
            return {}
        if value >= 35:
            return {"zone": "极度恐慌", "commentary": "波动率处于压力区，通常对应风险偏好显著收缩。"}
        if value >= 25:
            return {"zone": "恐慌", "commentary": "避险需求明显抬升，短线更要重视仓位和回撤控制。"}
        if value >= 18:
            return {"zone": "警戒", "commentary": "波动率进入警戒区，事件驱动更容易放大行情。"}
        if value >= 13:
            return {"zone": "中性", "commentary": "波动率处于常态区间，情绪没有明显失控。"}
        return {"zone": "平静", "commentary": "波动率偏低，通常说明风险偏好较高，但也要防范过度乐观。"}

    def _fetch_us_10y_radar(self, days: int = 180) -> Dict[str, Any]:
        history: List[Dict[str, Any]] = []
        try:
            with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                df = ak.bond_zh_us_rate()
            if df is not None and not df.empty:
                date_col = df.columns[0]
                us10y_col = next(
                    (
                        col for col in df.columns
                        if "美国" in str(col) and "国债收益率" in str(col) and "10" in str(col)
                    ),
                    None,
                )
                if us10y_col:
                    subset = df[[date_col, us10y_col]].copy()
                    subset[us10y_col] = pd.to_numeric(subset[us10y_col], errors="coerce")
                    subset = subset.dropna(subset=[us10y_col]).tail(days)
                    history = [
                        {"date": str(row[date_col])[:10], "value": round(float(row[us10y_col]), 2)}
                        for _, row in subset.iterrows()
                    ]
        except Exception as exc:
            print(f"美国10年期国债收益率历史获取失败: {exc}")

        payload = self._build_series_summary(history, source="akshare:bond_zh_us_rate", unit="%")
        if payload.get("latest") is not None and payload.get("delta") is not None:
            payload["delta_bps"] = round(payload["delta"] * 100, 1)
        payload.update(self._classify_us10y(payload.get("latest")))
        return payload

    def _fetch_vix_radar(self, days: int = 180) -> Dict[str, Any]:
        history: List[Dict[str, Any]] = []
        try:
            db_rows = self._get_db_service().get_vix_history(days)
            history = [
                {
                    "date": str(item.get("date"))[:10],
                    "value": round(float(item.get("close")), 2),
                }
                for item in db_rows
                if item.get("close") not in (None, "", "-", "--")
            ]
        except Exception as exc:
            print(f"VIX历史快照读取失败: {exc}")

        payload = self._build_series_summary(history, source="yahoo+sqlite:vix", unit="")
        realtime = self._fetch_vix_yahoo()
        if realtime and realtime.get("value") is not None:
            current_value = round(float(realtime["value"]), 2)
            prev_close = self._safe_float(realtime.get("prev_close"))
            payload.update(
                {
                    "latest": current_value,
                    "previous": prev_close,
                    "delta": round(current_value - prev_close, 2) if prev_close is not None else payload.get("delta"),
                    "delta_pct": realtime.get("change_pct"),
                    "as_of": (realtime.get("quote_time") or "")[:10] or payload.get("as_of"),
                    "source": "yahoo:vix",
                }
            )
        payload.update(self._classify_vix(payload.get("latest")))
        return payload

    def _fetch_pentagon_pizza_index(self) -> Dict[str, Any]:
        url = "https://www.pizzint.watch/"
        last_error = None

        for session in (self.session, self.direct_session):
            try:
                resp = session.get(url, timeout=20)
                resp.raise_for_status()
                html = resp.text

                level_match = re.search(r"DOUGHCON\s*(\d+)", html)
                details_match = re.search(
                    r"DOUGHCON\s*\d+</div><div[^>]*>\s*<span>([^<]+)</span><span[^>]*>.*?</span><span>([^<]+)</span>",
                    html,
                    re.S,
                )
                locations_match = re.search(r"(\d+)\s+LOCATIONS MONITORED", html)

                if not level_match:
                    return {}

                level = int(level_match.group(1))
                headline = details_match.group(1).strip() if details_match else None
                watch_status = details_match.group(2).strip() if details_match else None
                locations = int(locations_match.group(1)) if locations_match else None
                descriptions = {
                    1: "披萨店流量接近基线，未见明显异常。",
                    2: "出现轻微异动，适合留意外交或军事事件是否升温。",
                    3: "监测热度抬升，通常意味着政策或安全活动增多。",
                    4: "显著忙碌，往往对应更高强度的情报或安保活动。",
                    5: "极端异常，代表监测热度进入非常规状态。",
                }

                return {
                    "level": level,
                    "headline": headline,
                    "watch_status": watch_status,
                    "locations_monitored": locations,
                    "description": descriptions.get(level),
                    "as_of": datetime.now().replace(microsecond=0).isoformat(),
                    "source": "pizzint.watch",
                    "url": url,
                }
            except Exception as exc:
                last_error = exc

        if last_error:
            print(f"披萨指数获取失败: {last_error}")
        return {}

    def _fetch_shibor(self) -> Optional[Dict[str, Any]]:
        try:
            df = ak.macro_china_shibor_all()
            if df is None or df.empty:
                return None

            latest = df.iloc[-1]
            return {
                "date": str(latest.iloc[0])[:10],
                "overnight": self._safe_float(latest.iloc[1]),
                "1w": self._safe_float(latest.iloc[3]),
                "1m": self._safe_float(latest.iloc[7]),
                "3m": self._safe_float(latest.iloc[9]),
                "6m": self._safe_float(latest.iloc[11]),
                "1y": self._safe_float(latest.iloc[15]),
                "source": "akshare:macro_china_shibor_all",
            }
        except Exception as exc:
            print(f"SHIBOR失败: {exc}")
            return None

    def _fetch_hibor(self) -> Optional[Dict[str, Any]]:
        url = "https://www.hkab.org.hk/en/rates/hibor"

        last_error = None
        for session in (self.direct_session, self.session):
            try:
                resp = session.get(url, timeout=20)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
                rows = soup.select(".general_table_row")
                if not rows:
                    return None

                mapping: Dict[str, float] = {}
                for row in rows[1:]:
                    cells = [cell.get_text(" ", strip=True) for cell in row.select(".general_table_cell")]
                    if len(cells) < 2:
                        continue
                    maturity = cells[0]
                    value = self._safe_float(cells[1])
                    if value is None:
                        continue
                    mapping[maturity] = value

                if not mapping:
                    return None

                date_header = resp.headers.get("Date")
                quote_date = None
                if date_header:
                    try:
                        quote_date = parsedate_to_datetime(date_header).strftime("%Y-%m-%d")
                    except Exception:
                        quote_date = None

                return {
                    "date": quote_date or datetime.now().strftime("%Y-%m-%d"),
                    "overnight": mapping.get("Overnight"),
                    "1w": mapping.get("1 Week"),
                    "1m": mapping.get("1 Month"),
                    "3m": mapping.get("3 Months"),
                    "6m": mapping.get("6 Months"),
                    "source": "hkab",
                }
            except Exception as exc:
                last_error = exc

        if last_error:
            print(f"HIBOR失败: {last_error}")
        return None

    def _fetch_bond_yields(self) -> Dict[str, float]:
        result: Dict[str, float] = {}
        try:
            df = ak.bond_zh_us_rate()
            if df is None or df.empty:
                return result

            latest = df.iloc[-1]
            for col in df.columns:
                if "国债" in col and pd.notna(latest.get(col)):
                    result[col] = float(latest[col])
        except Exception as exc:
            print(f"国债收益率失败: {exc}")
        return result

    def _fetch_market_sentiment(self) -> Dict[str, Any]:
        raw: Dict[str, Any] = {}
        try:
            df = ak.stock_market_activity_legu()
            if df is None or df.empty:
                return {}
            for _, row in df.iterrows():
                raw[row["item"]] = row["value"]
        except Exception as exc:
            print(f"市场情绪失败: {exc}")
            return {}

        def _to_int(value: Any) -> Optional[int]:
            parsed = self._safe_float(value)
            return None if parsed is None else int(parsed)

        date_text = str(raw.get("统计日期") or "")[:10] or datetime.now().strftime("%Y-%m-%d")
        return {
            "date": date_text,
            "up_count": _to_int(raw.get("上涨")),
            "down_count": _to_int(raw.get("下跌")),
            "flat_count": _to_int(raw.get("平盘")),
            "limit_up_count": _to_int(raw.get("涨停") or raw.get("真实涨停")),
            "limit_down_count": _to_int(raw.get("跌停") or raw.get("真实跌停")),
            "raw": raw,
        }

    def get_market_overview(self, force_refresh: bool = False) -> Dict[str, Any]:
        if (
            not force_refresh
            and self._overview_cache is not None
            and self._cache_is_fresh(self._overview_cache_at, OVERVIEW_CACHE_TTL_SECONDS)
        ):
            cache_age = int(max(0, time.time() - self._overview_cache_at))
            storage = (self._overview_cache or {}).get("storage") or {}
            return self._attach_storage_meta(
                self._overview_cache,
                snapshot_key=OVERVIEW_SNAPSHOT_KEY,
                mode="memory_cache",
                source=storage.get("source") or "cache",
                updated_at=storage.get("updated_at"),
                age_seconds=cache_age,
                is_fresh=True,
                fetch_latency_ms=0,
                notes="命中进程内缓存，未触发外部实时拉取",
            )

        snapshot = self._get_db_service().get_market_snapshot(
            OVERVIEW_SNAPSHOT_KEY,
            OVERVIEW_CACHE_TTL_SECONDS,
        )
        if not force_refresh and snapshot and snapshot.get("payload"):
            is_fresh = bool(snapshot.get("is_fresh"))
            if not is_fresh:
                self._refresh_overview_async()
            cached = self._attach_storage_meta(
                snapshot["payload"],
                snapshot_key=OVERVIEW_SNAPSHOT_KEY,
                mode="sqlite_snapshot" if is_fresh else "sqlite_snapshot_stale",
                source=snapshot.get("source") or "snapshot",
                updated_at=snapshot.get("updated_at"),
                age_seconds=snapshot.get("age_seconds"),
                is_fresh=is_fresh,
                fetch_latency_ms=snapshot.get("fetch_latency_ms"),
                notes="直接命中 Windows 本地 SQLite 快照"
                if is_fresh
                else "本地快照已过期，当前请求先返回快照并异步触发实时刷新",
            )
            self._overview_cache = cached
            self._overview_cache_at = time.time()
            return cached

        fetch_started = time.perf_counter()
        last_error = None
        try:
            shibor = self._fetch_shibor()
            hibor = self._fetch_hibor()

            rates: Dict[str, Any] = {"bond": self._fetch_bond_yields()}
            if shibor:
                rates["date"] = shibor.get("date")
                rates["shibor"] = {
                    "overnight": shibor.get("overnight"),
                    "1w": shibor.get("1w"),
                    "1m": shibor.get("1m"),
                    "3m": shibor.get("3m"),
                    "6m": shibor.get("6m"),
                    "1y": shibor.get("1y"),
                }
            if hibor:
                rates["date"] = rates.get("date") or hibor.get("date")
                rates["hibor"] = {
                    "overnight": hibor.get("overnight"),
                    "1w": hibor.get("1w"),
                    "1m": hibor.get("1m"),
                    "3m": hibor.get("3m"),
                    "6m": hibor.get("6m"),
                }

            overview = {
                "update_time": datetime.now().isoformat(),
                "indices": self._fetch_realtime_indices(),
                "sentiment": self._fetch_market_sentiment(),
                "rates": rates,
                "fear_greed": {},
                "watch_stocks": self.get_watch_stocks(force_refresh=force_refresh),
            }

            vix = self._fetch_vix_yahoo()
            if vix:
                overview["fear_greed"]["vix"] = vix

            fetch_latency_ms = int((time.perf_counter() - fetch_started) * 1000)
            snapshot_meta = self._get_db_service().save_market_snapshot(
                OVERVIEW_SNAPSHOT_KEY,
                overview,
                OVERVIEW_CACHE_TTL_SECONDS,
                source="realtime",
                fetch_latency_ms=fetch_latency_ms,
            )
            overview = self._attach_storage_meta(
                overview,
                snapshot_key=OVERVIEW_SNAPSHOT_KEY,
                mode="realtime_refresh",
                source="realtime",
                updated_at=snapshot_meta.get("updated_at"),
                age_seconds=0,
                is_fresh=True,
                fetch_latency_ms=fetch_latency_ms,
                notes="实时拉取后写入 Windows 本地 SQLite 快照",
            )

            self._overview_cache = overview
            self._overview_cache_at = time.time()
            return overview
        except Exception as exc:
            last_error = exc
            if snapshot:
                cached = self._attach_storage_meta(
                    snapshot["payload"],
                    snapshot_key=OVERVIEW_SNAPSHOT_KEY,
                    mode="snapshot_fallback",
                    source=snapshot.get("source") or "snapshot",
                    updated_at=snapshot.get("updated_at"),
                    age_seconds=snapshot.get("age_seconds"),
                    is_fresh=snapshot.get("is_fresh"),
                    fetch_latency_ms=snapshot.get("fetch_latency_ms"),
                    notes=f"实时刷新失败，退回本地快照: {exc}",
                )
                self._overview_cache = cached
                self._overview_cache_at = time.time()
                return cached
            raise last_error

    def get_watch_stocks(self, force_refresh: bool = False) -> Dict[str, Any]:
        if (
            not force_refresh
            and self._watch_cache is not None
            and self._cache_is_fresh(self._watch_cache_at, WATCH_CACHE_TTL_SECONDS)
        ):
            cache_age = int(max(0, time.time() - self._watch_cache_at))
            storage = (self._watch_cache or {}).get("storage") or {}
            return self._attach_storage_meta(
                self._watch_cache,
                snapshot_key=WATCH_SNAPSHOT_KEY,
                mode="memory_cache",
                source=storage.get("source") or "cache",
                updated_at=storage.get("updated_at"),
                age_seconds=cache_age,
                is_fresh=True,
                fetch_latency_ms=0,
                notes="命中进程内缓存，未触发外部实时拉取",
            )

        snapshot = self._get_db_service().get_market_snapshot(
            WATCH_SNAPSHOT_KEY,
            WATCH_CACHE_TTL_SECONDS,
        )
        if not force_refresh and snapshot and snapshot.get("payload"):
            is_fresh = bool(snapshot.get("is_fresh"))
            if not is_fresh:
                self._refresh_watch_async()
            cached = self._attach_storage_meta(
                snapshot["payload"],
                snapshot_key=WATCH_SNAPSHOT_KEY,
                mode="sqlite_snapshot" if is_fresh else "sqlite_snapshot_stale",
                source=snapshot.get("source") or "snapshot",
                updated_at=snapshot.get("updated_at"),
                age_seconds=snapshot.get("age_seconds"),
                is_fresh=is_fresh,
                fetch_latency_ms=snapshot.get("fetch_latency_ms"),
                notes="直接命中 Windows 本地 SQLite 自选池快照"
                if is_fresh
                else "自选池快照已过期，当前请求先返回快照并异步触发实时刷新",
            )
            self._watch_cache = cached
            self._watch_cache_at = time.time()
            return cached

        fetch_started = time.perf_counter()
        try:
            data = self._fetch_watch_stocks_uncached()
            fetch_latency_ms = int((time.perf_counter() - fetch_started) * 1000)
            snapshot_meta = self._get_db_service().save_market_snapshot(
                WATCH_SNAPSHOT_KEY,
                data,
                WATCH_CACHE_TTL_SECONDS,
                source="realtime",
                fetch_latency_ms=fetch_latency_ms,
            )
            data = self._attach_storage_meta(
                data,
                snapshot_key=WATCH_SNAPSHOT_KEY,
                mode="realtime_refresh",
                source="realtime",
                updated_at=snapshot_meta.get("updated_at"),
                age_seconds=0,
                is_fresh=True,
                fetch_latency_ms=fetch_latency_ms,
                notes="实时拉取后写入 Windows 本地 SQLite 自选池快照",
            )
            self._watch_cache = data
            self._watch_cache_at = time.time()
            return data
        except Exception as exc:
            if snapshot:
                cached = self._attach_storage_meta(
                    snapshot["payload"],
                    snapshot_key=WATCH_SNAPSHOT_KEY,
                    mode="snapshot_fallback",
                    source=snapshot.get("source") or "snapshot",
                    updated_at=snapshot.get("updated_at"),
                    age_seconds=snapshot.get("age_seconds"),
                    is_fresh=snapshot.get("is_fresh"),
                    fetch_latency_ms=snapshot.get("fetch_latency_ms"),
                    notes=f"实时刷新失败，退回本地快照: {exc}",
                )
                self._watch_cache = cached
                self._watch_cache_at = time.time()
                return cached
            raise


    def _fetch_gold_radar(self, days: int = 180):
        """Gold price from local commodity_prices table."""
        history = []
        try:
            import sqlite3 as _sql
            conn = _sql.connect("data/investment.db")
            c = conn.cursor()
            rows = c.execute(
                "SELECT trade_date, price FROM commodity_prices "
                "WHERE commodity_type='gold' ORDER BY trade_date DESC LIMIT ?",
                (days,),
            ).fetchall()
            conn.close()
            history = [{"date": r[0], "value": round(float(r[1]), 2)} for r in reversed(rows) if r[1]]
        except Exception as exc:
            print(f"Gold history failed: {exc}")
        payload = self._build_series_summary(history, source="sqlite:commodity_prices", unit="CNY/g")
        payload.update(self._classify_gold(payload.get("latest")))
        return payload

    def _fetch_oil_radar(self, days: int = 180):
        """Oil price from local commodity_prices table."""
        history = []
        try:
            import sqlite3 as _sql
            conn = _sql.connect("data/investment.db")
            c = conn.cursor()
            rows = c.execute(
                "SELECT trade_date, price FROM commodity_prices "
                "WHERE commodity_type='oil' ORDER BY trade_date DESC LIMIT ?",
                (days,),
            ).fetchall()
            conn.close()
            history = [{"date": r[0], "value": round(float(r[1]), 2)} for r in reversed(rows) if r[1]]
        except Exception as exc:
            print(f"Oil history failed: {exc}")
        payload = self._build_series_summary(history, source="sqlite:commodity_prices", unit="CNY/bbl")
        payload.update(self._classify_oil(payload.get("latest")))
        return payload

    def _fetch_yield_spread_radar(self, days: int = 180):
        """US 10Y-2Y yield spread."""
        history = []
        try:
            import sqlite3 as _sql
            conn = _sql.connect("data/investment.db")
            c = conn.cursor()
            rows = c.execute(
                "SELECT trade_date, us_10y_2y_spread FROM us_treasury_history "
                "WHERE us_10y_2y_spread IS NOT NULL ORDER BY trade_date DESC LIMIT ?",
                (days,),
            ).fetchall()
            conn.close()
            history = [{"date": r[0], "value": round(float(r[1]), 2)} for r in reversed(rows) if r[1] is not None]
        except Exception as exc:
            print(f"Yield spread failed: {exc}")
        payload = self._build_series_summary(history, source="sqlite:us_treasury_history", unit="bps")
        payload.update(self._classify_yield_spread(payload.get("latest")))
        return payload

    def _fetch_dxy_radar(self, days: int = 180):
        """US Dollar Index via yfinance."""
        history = []
        try:
            import yfinance as yf
            df = yf.download("DX-Y.NYB", period=f"{min(days, 365)}d", progress=False)
            if df is not None and not df.empty:
                for date_idx, row in df.iterrows():
                    try:
                        cv = float(row["Close"].iloc[0]) if hasattr(row["Close"], "iloc") else float(row["Close"])
                    except Exception:
                        cv = float(row["Close"])
                    history.append({"date": date_idx.strftime("%Y-%m-%d"), "value": round(cv, 2)})
        except Exception as exc:
            print(f"DXY failed: {exc}")
        payload = self._build_series_summary(history, source="yahoo:DX-Y.NYB", unit="")
        payload.update(self._classify_dxy(payload.get("latest")))
        return payload

    def _classify_gold(self, value):
        if value is None: return {}
        # CNY/gram: 1000+ extreme, 800+ elevated, 600+ neutral
        if value >= 1000: return {"zone": "risk_haven_surge", "commentary": "\u9ec4\u91d1\u6781\u7aef\u9ad8\u4f4d\uff0c\u907f\u9669\u9700\u6c42\u5f3a\u70c8"}
        if value >= 800: return {"zone": "elevated", "commentary": "\u9ec4\u91d1\u504f\u9ad8\uff0c\u53cd\u6620\u901a\u80c0\u5bf9\u51b2\u6216\u5730\u7f18\u4e0d\u786e\u5b9a\u6027"}
        if value >= 600: return {"zone": "neutral", "commentary": "\u9ec4\u91d1\u6b63\u5e38\u533a\u95f4"}
        return {"zone": "low", "commentary": "\u9ec4\u91d1\u4f4e\u8ff7\uff0c\u98ce\u9669\u504f\u597d\u8f83\u5f3a"}

    def _classify_oil(self, value):
        if value is None: return {}
        # CNY/barrel: 700+ supply shock, 550+ elevated, 400+ neutral
        if value >= 700: return {"zone": "supply_shock", "commentary": "\u6cb9\u4ef7\u9ad8\u4f4d\uff0c\u6ede\u80c0\u98ce\u9669\u4e0a\u5347"}
        if value >= 550: return {"zone": "elevated", "commentary": "\u6cb9\u4ef7\u504f\u9ad8\uff0c\u5173\u6ce8\u901a\u80c0\u4f20\u5bfc"}
        if value >= 400: return {"zone": "neutral", "commentary": "\u6cb9\u4ef7\u5747\u8861\u533a\u95f4"}
        return {"zone": "demand_weakness", "commentary": "\u6cb9\u4ef7\u4f4e\u8ff7\uff0c\u9700\u6c42\u653e\u7f13\u4fe1\u53f7"}

    def _classify_yield_spread(self, value):
        if value is None: return {}
        if value < -0.5: return {"zone": "deep_inversion", "commentary": "\u6536\u76ca\u7387\u66f2\u7ebf\u6df1\u5ea6\u5012\u6302\uff0c\u8870\u9000\u9884\u8b66"}
        if value < 0: return {"zone": "inverted", "commentary": "\u6536\u76ca\u7387\u66f2\u7ebf\u5012\u6302\uff0c\u8870\u9000\u4fe1\u53f7"}
        if value < 0.5: return {"zone": "flat", "commentary": "\u6536\u76ca\u7387\u66f2\u7ebf\u5e73\u5766"}
        return {"zone": "normal", "commentary": "\u6536\u76ca\u7387\u66f2\u7ebf\u6b63\u5e38"}

    def _classify_dxy(self, value):
        if value is None: return {}
        if value >= 108: return {"zone": "strong_dollar", "commentary": "\u5f3a\u7f8e\u5143\u538b\u5236\u65b0\u5174\u5e02\u573a\u548c\u5546\u54c1"}
        if value >= 103: return {"zone": "firm", "commentary": "\u7f8e\u5143\u504f\u5f3a"}
        if value >= 97: return {"zone": "neutral", "commentary": "\u7f8e\u5143\u4e2d\u6027\u533a\u95f4"}
        return {"zone": "weak_dollar", "commentary": "\u5f31\u7f8e\u5143\u652f\u6491\u65b0\u5174\u5e02\u573a\u548c\u5546\u54c1"}

    def _compute_composite_risk_score(self, radar):
        score = 50.0
        signals = []
        vix = self._safe_float(radar.get('vix', {}).get('latest'))
        if vix is not None:
            if vix >= 35: score += 15; signals.append(('VIX', 'extreme_fear', 15))
            elif vix >= 25: score += 8; signals.append(('VIX', 'fear', 8))
            elif vix >= 18: score += 2; signals.append(('VIX', 'caution', 2))
            elif vix < 13: score -= 8; signals.append(('VIX', 'complacent', -8))
            else: signals.append(('VIX', 'neutral', 0))
        us10y = self._safe_float(radar.get('us10y', {}).get('latest'))
        if us10y is not None:
            if us10y >= 5.0: score += 10; signals.append(('US10Y', 'very_tight', 10))
            elif us10y >= 4.5: score += 5; signals.append(('US10Y', 'tight', 5))
            elif us10y < 3.5: score -= 5; signals.append(('US10Y', 'accommodative', -5))
            else: signals.append(('US10Y', 'neutral', 0))
        spread = self._safe_float(radar.get('yield_spread', {}).get('latest'))
        if spread is not None:
            if spread < -0.5: score += 10; signals.append(('YieldSpread', 'deep_inversion', 10))
            elif spread < 0: score += 5; signals.append(('YieldSpread', 'inverted', 5))
            else: signals.append(('YieldSpread', 'normal', 0))
        dxy = self._safe_float(radar.get('dxy', {}).get('latest'))
        if dxy is not None:
            if dxy >= 108: score += 8; signals.append(('DXY', 'strong', 8))
            elif dxy < 97: score -= 5; signals.append(('DXY', 'weak', -5))
            else: signals.append(('DXY', 'neutral', 0))
        gold = self._safe_float(radar.get('gold', {}).get('latest'))
        if gold is not None:
            if gold >= 1000: score += 5; signals.append(('Gold', 'haven_surge', 5))
            elif gold < 600: score -= 3; signals.append(('Gold', 'low', -3))
            else: signals.append(('Gold', 'neutral', 0))
        oil = self._safe_float(radar.get('oil', {}).get('latest'))
        if oil is not None:
            if oil >= 700: score += 8; signals.append(('Oil', 'supply_shock', 8))
            elif oil < 350: score += 5; signals.append(('Oil', 'demand_collapse', 5))
            else: signals.append(('Oil', 'neutral', 0))
        score = max(0, min(100, score))
        if score >= 75: level, label, color = 'extreme', 'Extreme Risk', '#ef4444'
        elif score >= 60: level, label, color = 'high', 'High Risk', '#f97316'
        elif score >= 40: level, label, color = 'moderate', 'Moderate', '#eab308'
        elif score >= 25: level, label, color = 'low', 'Low Risk', '#84cc16'
        else: level, label, color = 'very_low', 'Very Low Risk', '#22c55e'
        return {'score': round(score, 1), 'level': level, 'label': label, 'color': color, 'signals': signals}

    def get_global_risk_radar(self, days: int = 180, force_refresh: bool = False) -> Dict[str, Any]:
        if (
            not force_refresh
            and self._global_risk_cache is not None
            and self._cache_is_fresh(self._global_risk_cache_at, GLOBAL_RISK_CACHE_TTL_SECONDS)
        ):
            cached_days = self._global_risk_cache.get("days", 0)
            if cached_days >= days:
                return {
                    **self._global_risk_cache,
                    }
                trimmed = dict(self._global_risk_cache)
                for _k in ("us10y", "vix", "gold", "oil", "yield_spread", "dxy"):
                    if _k in trimmed and "history" in trimmed[_k]:
                        trimmed[_k] = {**trimmed[_k], "history": trimmed[_k]["history"][-days:]}
                return trimmed

        fetch_days = max(days, 180)
        payload = {
            "update_time": datetime.now().replace(microsecond=0).isoformat(),
            "days": fetch_days,
            "us10y": self._fetch_us_10y_radar(days=fetch_days),
            "vix": self._fetch_vix_radar(days=fetch_days),
            "pentagon_pizza": self._fetch_pentagon_pizza_index(),
            "gold": self._fetch_gold_radar(days=fetch_days),
            "oil": self._fetch_oil_radar(days=fetch_days),
            "yield_spread": self._fetch_yield_spread_radar(days=fetch_days),
            "dxy": self._fetch_dxy_radar(days=fetch_days),
        }
        payload["composite_risk"] = self._compute_composite_risk_score(payload)
        self._global_risk_cache = payload
        self._global_risk_cache_at = time.time()

        trimmed = dict(payload)
        for _k in ("us10y", "vix", "gold", "oil", "yield_spread", "dxy"):
            if _k in trimmed and "history" in trimmed[_k]:
                trimmed[_k] = {**trimmed[_k], "history": trimmed[_k]["history"][-days:]}
        return trimmed

    def get_hk_stocks_direct(self, keywords: List[str] = None) -> List[Dict[str, Any]]:
        stocks = self.get_watch_stocks().get("hk_stocks", [])
        if not keywords:
            return stocks
        return [item for item in stocks if any(keyword in item["name"] for keyword in keywords)]

    def get_a_stocks_direct(self, keywords: List[str] = None) -> List[Dict[str, Any]]:
        stocks = self.get_watch_stocks().get("a_stocks", [])
        if not keywords:
            return stocks
        return [item for item in stocks if any(keyword in item["name"] for keyword in keywords)]

    def get_index_history(self, symbol: str, days: int = 365) -> List[Dict[str, Any]]:
        history = []
        try:
            df = ak.stock_zh_index_daily(symbol=symbol)
            if df is not None and not df.empty:
                df = df.tail(days)
                for _, row in df.iterrows():
                    history.append({"date": str(row["date"]), "close": float(row["close"])})
        except Exception as exc:
            print(f"指数历史失败: {exc}")
        return history

    def get_hsi_history(self, days: int = 365) -> List[Dict[str, Any]]:
        history = []
        try:
            df = ak.stock_hk_index_daily_em(symbol="HSI")
            if df is not None and not df.empty:
                df = df.tail(days)
                for _, row in df.iterrows():
                    history.append({"date": str(row["date"]), "close": float(row["close"])})
        except Exception as exc:
            print(f"恒生指数历史失败: {exc}")
        return history

    # =============================================================================
    # 港股数据增强
    # =============================================================================

    def get_hk_stock_hot_rank(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        港股热门排行（东方财富）
        """
        try:
            df = ak.stock_hk_hot_rank_em()
            if df is None or df.empty:
                return []
            result = []
            for _, row in df.head(limit).iterrows():
                code = str(row.get("代码", ""))
                result.append({
                    "code": f"hk{code}",
                    "name": str(row.get("股票名称", "")),
                    "price": self._safe_float(row.get("最新价")),
                    "change_pct": self._safe_float(row.get("涨跌幅")),
                    "volume": self._safe_int(row.get("成交量")),
                    "turnover": self._safe_float(row.get("成交额")),
                })
            return result
        except Exception as exc:
            print(f"港股热榜失败: {exc}")
            return []

    def get_hk_stock_financial(self, symbol: str) -> Dict[str, Any]:
        """
        港股财务数据（东方财富）
        symbol 格式: "00700", "09988" 等
        """
        try:
            df = ak.stock_financial_hk_analysis_indicator_em(symbol=symbol)
            if df is None or df.empty:
                return {}
            latest = df.iloc[-1]
            return {
                "pe_ttm": self._safe_float(latest.get("市盈率")),
                "pb": self._safe_float(latest.get("市净率")),
                "roe": self._safe_float(latest.get("净资产收益率")),
                "dividend_yield": self._safe_float(latest.get("股息率")),
                "eps": self._safe_float(latest.get("每股收益")),
                "revenue": self._safe_float(latest.get("营业收入")),
                "net_profit": self._safe_float(latest.get("净利润")),
            }
        except Exception as exc:
            print(f"港股财务数据获取失败 {symbol}: {exc}")
            return {}

    def get_hk_stock_history(self, symbol: str, days: int = 365) -> List[Dict[str, Any]]:
        """
        港股历史行情（东方财富）
        symbol 格式: "00700"
        """
        try:
            df = ak.stock_hk_hist_daily_em(symbol=symbol)
            if df is None or df.empty:
                return []
            df = df.tail(days)
            return [
                {
                    "date": str(row.get("date", ""))[:10],
                    "open": self._safe_float(row.get("open")),
                    "high": self._safe_float(row.get("high")),
                    "low": self._safe_float(row.get("low")),
                    "close": self._safe_float(row.get("close")),
                    "volume": self._safe_float(row.get("volume")),
                }
                for _, row in df.iterrows()
            ]
        except Exception as exc:
            print(f"港股历史行情获取失败 {symbol}: {exc}")
            return []

    def get_hk_index_list(self) -> List[Dict[str, Any]]:
        """
        获取港股主要指数行情
        """
        indices = [
            ("HSI", "恒生指数"),
            ("HSTECH", "恒生科技"),
            ("HSCEI", "恒生中国企业"),
            ("HSSCI", "恒生综合指数"),
        ]
        result = []
        for symbol, name in indices:
            try:
                df = ak.stock_hk_index_daily_em(symbol=symbol)
                if df is not None and not df.empty:
                    latest = df.iloc[-1]
                    prev = df.iloc[-2] if len(df) >= 2 else latest
                    close = self._safe_float(latest.get("close"))
                    prev_close = self._safe_float(prev.get("close"))
                    result.append({
                        "code": symbol,
                        "name": name,
                        "close": close,
                        "change_pct": self._compute_change_pct(close, prev_close),
                        "date": str(latest.get("date", ""))[:10],
                    })
            except Exception as exc:
                print(f"港股指数 {symbol} 获取失败: {exc}")
        return result

    def get_hk_repurchase_stats(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        港股回购统计（重要信号）
        """
        try:
            df = ak.stock_hk_repurchase_em()
            if df is None or df.empty:
                return []
            df = df.head(days)
            # 按公司聚合
            company_stats = {}
            for _, row in df.iterrows():
                name = str(row.get("公司名称", ""))
                if name not in company_stats:
                    company_stats[name] = {
                        "name": name,
                        "code": str(row.get("代码", "")),
                        "total_amount": 0,
                        "count": 0,
                    }
                company_stats[name]["total_amount"] += self._safe_float(row.get("回购金额")) or 0
                company_stats[name]["count"] += 1

            result = sorted(company_stats.values(), key=lambda x: x["total_amount"], reverse=True)
            return result[:30]
        except Exception as exc:
            print(f"港股回购数据获取失败: {exc}")
            return []


if __name__ == "__main__":
    service = InvestmentDataService()
    overview = service.get_market_overview(force_refresh=True)

    print("=== 投资数据服务测试 ===")
    print(f"更新时间: {overview['update_time']}")
    print("\n指数:")
    for value in overview["indices"].values():
        print(
            f"  {value['name']}: {value['close']} ({value.get('change_pct')}%) "
            f"[{value.get('source')}, 校验={value.get('validated')}]"
        )

    vix = overview.get("fear_greed", {}).get("vix")
    if vix:
        print(f"\nVIX: {vix['value']} ({vix.get('change_pct')}%) [{vix.get('source')}]")

    if overview["rates"].get("shibor"):
        print(f"\nSHIBOR隔夜: {overview['rates']['shibor']['overnight']}")
    if overview["rates"].get("hibor"):
        print(f"HIBOR隔夜: {overview['rates']['hibor']['overnight']}")

    watch = overview["watch_stocks"]
    print(f"\nA股自选: {len(watch['a_stocks'])} 只")
    print(f"港股自选: {len(watch['hk_stocks'])} 只")
