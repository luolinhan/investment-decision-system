"""
研报下载系统 - 投资决策数据服务（完整版）
包含：市场指数、恐慌指数、利率、股票行情
"""
import os
import requests
import re
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any

os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'

import akshare as ak

# 关注的股票列表
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
    }
}


class InvestmentDataService:
    """投资决策数据服务"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://gu.qq.com/',
        })

    def _get_tencent_quote(self, codes: List[str]) -> str:
        """从腾讯财经获取实时行情"""
        url = "https://qt.gtimg.cn/q=" + ",".join(codes)
        try:
            resp = self.session.get(url, timeout=10)
            resp.encoding = "gbk"
            return resp.text
        except Exception as e:
            return ""

    def _parse_a_stock(self, data: str) -> Dict:
        parts = data.split("~")
        if len(parts) < 35:
            return None
        try:
            return {
                "name": parts[1],
                "code": parts[2],
                "price": float(parts[3]) if parts[3] else 0,
                "prev_close": float(parts[4]) if parts[4] else 0,
                "open": float(parts[5]) if parts[5] else 0,
                "change": float(parts[31]) if len(parts) > 31 and parts[31] else 0,
                "change_pct": float(parts[32]) if len(parts) > 32 and parts[32] else 0,
                "high": float(parts[33]) if len(parts) > 33 and parts[33] else 0,
                "low": float(parts[34]) if len(parts) > 34 and parts[34] else 0,
                "volume": int(float(parts[6])) if len(parts) > 6 and parts[6] else 0,
            }
        except (ValueError, IndexError):
            return None

    def _parse_hk_stock(self, data: str) -> Dict:
        parts = data.split("~")
        if len(parts) < 35:
            return None
        try:
            return {
                "name": parts[1],
                "code": parts[2],
                "price": float(parts[3]) if parts[3] else 0,
                "prev_close": float(parts[4]) if parts[4] else 0,
                "open": float(parts[5]) if parts[5] else 0,
                "change": float(parts[31]) if len(parts) > 31 and parts[31] else 0,
                "change_pct": float(parts[32]) if len(parts) > 32 and parts[32] else 0,
                "high": float(parts[33]) if len(parts) > 33 and parts[33] else 0,
                "low": float(parts[34]) if len(parts) > 34 and parts[34] else 0,
            }
        except (ValueError, IndexError):
            return None

    def _parse_quote(self, text: str, code: str) -> Dict:
        pattern = rf'v_{code}="(.*)";'
        match = re.search(pattern, text)
        if not match:
            return None
        data = match.group(1)
        if not data:
            return None
        if code.startswith('hk'):
            return self._parse_hk_stock(data)
        else:
            return self._parse_a_stock(data)

    def get_market_overview(self) -> Dict[str, Any]:
        """获取市场概览数据"""
        overview = {
            "update_time": datetime.now().isoformat(),
            "indices": {},
            "sentiment": {},
            "rates": {},
            "fear_greed": {},
        }

        # 1. A股指数
        index_codes = {
            "sh000001": "上证指数",
            "sz399001": "深证成指",
            "sz399006": "创业板指",
        }
        text = self._get_tencent_quote(list(index_codes.keys()))
        for code, name in index_codes.items():
            q = self._parse_quote(text, code)
            if q:
                overview["indices"][code] = {
                    "name": name,
                    "code": code,
                    "close": q["price"],
                    "change_pct": q.get("change_pct", 0),
                }

        # 2. 恒生指数
        text = self._get_tencent_quote(["hkHSI"])
        q = self._parse_quote(text, "hkHSI")
        if q:
            overview["indices"]["hkHSI"] = {
                "name": "恒生指数",
                "code": "hkHSI",
                "close": q["price"],
                "change_pct": q.get("change_pct", 0),
            }

        # 3. 道琼斯指数
        text = self._get_tencent_quote(["usDJI"])
        q = self._parse_quote(text, "usDJI")
        if q:
            overview["indices"]["usDJI"] = {
                "name": "道琼斯",
                "code": "usDJI",
                "close": q["price"],
                "change_pct": q.get("change_pct", 0),
            }

        # 4. 纳斯达克指数
        text = self._get_tencent_quote(["usIXIC"])
        q = self._parse_quote(text, "usIXIC")
        if q:
            overview["indices"]["usIXIC"] = {
                "name": "纳斯达克",
                "code": "usIXIC",
                "close": q["price"],
                "change_pct": q.get("change_pct", 0),
            }

        # 5. VIX恐慌指数
        try:
            text = self._get_tencent_quote(["usVIX"])
            q = self._parse_quote(text, "usVIX")
            if q:
                overview["fear_greed"]["vix"] = {
                    "name": "VIX恐慌指数",
                    "value": q["price"],
                    "change_pct": q.get("change_pct", 0),
                }
        except Exception as e:
            print(f"VIX获取失败: {e}")

        # 6. 市场情绪（A股涨跌统计）
        try:
            df = ak.stock_market_activity_legu()
            if len(df) > 0:
                for _, row in df.iterrows():
                    overview["sentiment"][row['item']] = row['value']
        except Exception as e:
            print(f"市场情绪失败: {e}")

        # 7. SHIBOR利率
        try:
            df = ak.rate_interbank(market="Shibor", symbol="Shibor人民币报价")
            if len(df) > 0:
                latest = df.tail(1).iloc[0]
                overview["rates"]["shibor"] = {
                    "overnight": float(latest.get('隔夜', 0)) if pd.notna(latest.get('隔夜')) else 0,
                    "1week": float(latest.get('1周', 0)) if pd.notna(latest.get('1周')) else 0,
                    "1month": float(latest.get('1个月', 0)) if pd.notna(latest.get('1个月')) else 0,
                }
        except Exception as e:
            print(f"SHIBOR失败: {e}")

        # 8. HIBOR利率
        try:
            df = ak.rate_hk_interbank(symbol="HIBOR")
            if len(df) > 0:
                latest = df.tail(1).iloc[0]
                overview["rates"]["hibor"] = {
                    "overnight": float(latest.get('隔夜', 0)) if pd.notna(latest.get('隔夜')) else 0,
                    "1week": float(latest.get('1周', 0)) if pd.notna(latest.get('1周')) else 0,
                    "1month": float(latest.get('1个月', 0)) if pd.notna(latest.get('1个月')) else 0,
                }
        except Exception as e:
            print(f"HIBOR失败: {e}")

        # 9. 国债收益率
        try:
            df = ak.bond_zh_us_rate()
            if len(df) > 0:
                latest = df.tail(1).iloc[0]
                overview["rates"]["bond"] = {}
                for col in df.columns:
                    if '国债' in col and pd.notna(latest.get(col)):
                        overview["rates"]["bond"][col] = float(latest[col])
        except Exception as e:
            print(f"国债收益率失败: {e}")

        return overview

    def get_watch_stocks(self) -> Dict[str, List[Dict[str, Any]]]:
        """获取关注股票的实时行情"""
        result = {
            "a_stocks": [],
            "hk_stocks": [],
            "update_time": datetime.now().isoformat(),
        }

        all_codes = list(WATCH_LIST["a_stocks"].values()) + list(WATCH_LIST["hk_stocks"].values())
        text = self._get_tencent_quote(all_codes)

        for name, code in WATCH_LIST["a_stocks"].items():
            q = self._parse_quote(text, code)
            if q:
                q["name"] = name
                result["a_stocks"].append(q)

        for name, code in WATCH_LIST["hk_stocks"].items():
            q = self._parse_quote(text, code)
            if q:
                q["name"] = name
                result["hk_stocks"].append(q)

        return result

    def get_hk_stocks_direct(self, keywords: List[str] = None) -> List[Dict[str, Any]]:
        stocks = []
        all_codes = list(WATCH_LIST["hk_stocks"].values())
        text = self._get_tencent_quote(all_codes)

        for name, code in WATCH_LIST["hk_stocks"].items():
            if keywords and not any(kw in name for kw in keywords):
                continue
            q = self._parse_quote(text, code)
            if q:
                q["name"] = name
                stocks.append(q)
        return stocks

    def get_a_stocks_direct(self, keywords: List[str] = None) -> List[Dict[str, Any]]:
        stocks = []
        all_codes = list(WATCH_LIST["a_stocks"].values())
        text = self._get_tencent_quote(all_codes)

        for name, code in WATCH_LIST["a_stocks"].items():
            if keywords and not any(kw in name for kw in keywords):
                continue
            q = self._parse_quote(text, code)
            if q:
                q["name"] = name
                stocks.append(q)
        return stocks

    def get_index_history(self, symbol: str, days: int = 365) -> List[Dict[str, Any]]:
        history = []
        try:
            df = ak.stock_zh_index_daily(symbol=symbol)
            if len(df) > 0:
                df = df.tail(days)
                for _, row in df.iterrows():
                    history.append({
                        "date": str(row['date']),
                        "close": float(row['close']),
                    })
        except Exception as e:
            print(f"指数历史失败: {e}")
        return history

    def get_hsi_history(self, days: int = 365) -> List[Dict[str, Any]]:
        history = []
        try:
            df = ak.stock_hk_index_daily_em(symbol="HSI")
            if len(df) > 0:
                df = df.tail(days)
                for _, row in df.iterrows():
                    history.append({
                        "date": str(row['date']),
                        "close": float(row['close']),
                    })
        except Exception as e:
            print(f"恒生指数历史失败: {e}")
        return history


if __name__ == "__main__":
    service = InvestmentDataService()

    print("=== 投资数据服务测试 ===\n")

    print("1. 市场数据:")
    overview = service.get_market_overview()

    print("   指数:")
    for key, value in overview["indices"].items():
        print(f"     {value['name']}: {value['close']} ({value['change_pct']}%)")

    print("   恐慌指数:")
    if "vix" in overview.get("fear_greed", {}):
        vix = overview["fear_greed"]["vix"]
        print(f"     {vix['name']}: {vix['value']} ({vix['change_pct']}%)")

    print("   利率:")
    if "shibor" in overview.get("rates", {}):
        print(f"     SHIBOR隔夜: {overview['rates']['shibor']['overnight']}")
    if "hibor" in overview.get("rates", {}):
        print(f"     HIBOR隔夜: {overview['rates']['hibor']['overnight']}")

    print("\n2. 关注股票:")
    watch = service.get_watch_stocks()
    print(f"   A股: {len(watch['a_stocks'])} 只")
    print(f"   港股: {len(watch['hk_stocks'])} 只")