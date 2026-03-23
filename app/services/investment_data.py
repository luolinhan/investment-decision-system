"""
投资决策数据服务

目标:
1. 当前行情默认走实时源, 避免受日更库表影响
2. 使用备用源做轻量交叉校验, 发现异常时回退
3. 为前端提供 5 分钟级缓存, 和页面刷新节奏保持一致
"""
import os
import re
import time
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

os.environ["HTTP_PROXY"] = "http://127.0.0.1:7890"
os.environ["HTTPS_PROXY"] = "http://127.0.0.1:7890"

import akshare as ak

WATCH_LIST = {
    "a_stocks": {
        "晶澳科技": "sz002459",
        "通威股份": "sh600438",
        "隆基绿能": "sh601012",
        "锦浪科技": "sz300763",
        "百济神州": "sh688235",
        "药明康德": "sh603259",
        "复星医药": "sh600196",
        "中国中免": "sh601888",
    },
    "hk_stocks": {
        "阿里巴巴": "hk09988",
        "腾讯": "hk00700",
        "美团": "hk03690",
        "小米": "hk01810",
        "快手": "hk01024",
        "百济神州": "hk06160",
        "药明生物": "hk02269",
        "中国海洋石油": "hk00883",
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
    "hsi": {"name": "恒生指数", "primary": "hkHSI", "secondary": "rt_hkHSI"},
    "dji": {"name": "道琼斯", "primary": "usDJI", "secondary": "gb_dji"},
    "ixic": {"name": "纳斯达克", "primary": "usIXIC", "secondary": "gb_ixic"},
    "inx": {"name": "标普500", "primary": "usSPX", "secondary": "gb_spx", "yahoo": "^GSPC"},
}

OVERVIEW_CACHE_TTL_SECONDS = 300
WATCH_CACHE_TTL_SECONDS = 300
QUOTE_VALIDATION_TOLERANCE = 0.005
VIX_VALIDATION_TOLERANCE = 0.10


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
            "update_time": datetime.now().isoformat(),
        }

        a_codes = list(WATCH_LIST["a_stocks"].values())
        hk_codes = list(WATCH_LIST["hk_stocks"].values())

        primary_text = self._get_tencent_quote(a_codes + hk_codes)
        secondary_text = self._get_sina_quote(a_codes + hk_codes)

        for name, code in WATCH_LIST["a_stocks"].items():
            primary = self._parse_tencent_quote(primary_text, code)
            secondary = self._parse_sina_a_quote(secondary_text, code)
            merged = self._merge_with_secondary(primary, secondary)
            if not merged or merged.get("price") is None:
                continue
            merged["name"] = name
            result["a_stocks"].append(merged)

        for name, code in WATCH_LIST["hk_stocks"].items():
            primary = self._parse_tencent_quote(primary_text, code)
            secondary = self._parse_sina_hk_quote(secondary_text, code)
            merged = self._merge_with_secondary(primary, secondary)
            if not merged or merged.get("price") is None:
                continue
            merged["name"] = name
            result["hk_stocks"].append(merged)

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
            return self._overview_cache

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

        self._overview_cache = overview
        self._overview_cache_at = time.time()
        return overview

    def get_watch_stocks(self, force_refresh: bool = False) -> Dict[str, Any]:
        if (
            not force_refresh
            and self._watch_cache is not None
            and self._cache_is_fresh(self._watch_cache_at, WATCH_CACHE_TTL_SECONDS)
        ):
            return self._watch_cache

        data = self._fetch_watch_stocks_uncached()
        self._watch_cache = data
        self._watch_cache_at = time.time()
        return data

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
