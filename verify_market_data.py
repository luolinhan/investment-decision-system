"""
校验当前市场数据是否和备用源一致

默认用于人工验收:
1. A / HK / US 指数对比新浪
2. VIX 对比 Yahoo
3. SHIBOR 对比 akshare
4. HIBOR 对比 HKAB 官方页面
"""
from datetime import datetime

import akshare as ak
import requests
from bs4 import BeautifulSoup

from app.services.investment_data import INDEX_CODES, InvestmentDataService

YAHOO_INDEX_SYMBOLS = {
    "inx": "^GSPC",
}

def compare_number(name: str, actual, expected, tolerance_pct: float = 0.5):
    if actual in (None, "") or expected in (None, ""):
        print(f"[WARN] {name}: 缺少对比值 actual={actual} expected={expected}")
        return None

    diff_pct = abs(actual - expected) / abs(expected) * 100 if expected else 0
    ok = diff_pct <= tolerance_pct
    print(
        f"[{'OK' if ok else 'FAIL'}] {name}: actual={actual:.4f} "
        f"expected={expected:.4f} diff={diff_pct:.3f}%"
    )
    return ok


def fetch_yahoo_price(symbol: str) -> float:
    response = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
        params={"range": "1d", "interval": "1m"},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20,
    )
    response.raise_for_status()
    result = response.json()["chart"]["result"][0]
    return float(result["meta"]["regularMarketPrice"])


def verify_indices(service: InvestmentDataService, overview: dict) -> int:
    print("\n=== 指数校验 ===")
    failures = 0
    text = service._get_sina_quote([item["secondary"] for item in INDEX_CODES.values()])

    for code, meta in INDEX_CODES.items():
        current = overview.get("indices", {}).get(code, {})
        if code in YAHOO_INDEX_SYMBOLS:
            expected_price = fetch_yahoo_price(YAHOO_INDEX_SYMBOLS[code])
        else:
            secondary_code = meta["secondary"]
            if secondary_code.startswith("s_"):
                expected = service._parse_sina_simple_index(text, secondary_code)
            elif secondary_code.startswith("rt_hk"):
                expected = service._parse_sina_hk_index(text, secondary_code)
            else:
                expected = service._parse_sina_us_index(text, secondary_code)
            expected_price = expected.get("price") if expected else None

        result = compare_number(meta["name"], current.get("close"), expected_price)
        if result is False:
            failures += 1
        elif result is None and current.get("close") is None:
            failures += 1

    return failures


def verify_vix(overview: dict) -> int:
    print("\n=== VIX 校验（Yahoo） ===")
    response = requests.get(
        "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX",
        params={"range": "1d", "interval": "1m"},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20,
    )
    response.raise_for_status()
    result = response.json()["chart"]["result"][0]
    expected = float(result["meta"]["regularMarketPrice"])
    actual = overview.get("fear_greed", {}).get("vix", {}).get("value")
    result = compare_number("VIX", actual, expected, tolerance_pct=0.5)
    return 0 if result in (True, None) else 1


def verify_shibor(overview: dict) -> int:
    print("\n=== SHIBOR 校验（akshare） ===")
    df = ak.macro_china_shibor_all()
    latest = df.iloc[-1]
    expected = {
        "overnight": float(latest.iloc[1]),
        "1w": float(latest.iloc[3]),
        "1m": float(latest.iloc[7]),
    }

    actual = overview.get("rates", {}).get("shibor", {})
    failures = 0
    for key, value in expected.items():
        result = compare_number(f"SHIBOR {key}", actual.get(key), value, tolerance_pct=0.1)
        if result is False or (result is None and actual.get(key) is None):
            failures += 1
    return failures


def verify_hibor(overview: dict) -> int:
    print("\n=== HIBOR 校验（HKAB） ===")
    html = requests.get(
        "https://www.hkab.org.hk/en/rates/hibor",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20,
    ).text
    soup = BeautifulSoup(html, "html.parser")
    mapping = {}
    for row in soup.select(".general_table_row")[1:]:
        cells = [cell.get_text(" ", strip=True) for cell in row.select(".general_table_cell")]
        if len(cells) >= 2:
            mapping[cells[0]] = float(cells[1])

    expected = {
        "overnight": mapping.get("Overnight"),
        "1w": mapping.get("1 Week"),
        "1m": mapping.get("1 Month"),
    }
    actual = overview.get("rates", {}).get("hibor", {})

    failures = 0
    for key, value in expected.items():
        result = compare_number(f"HIBOR {key}", actual.get(key), value, tolerance_pct=0.1)
        if result is False or (result is None and actual.get(key) is None):
            failures += 1
    return failures


def main():
    print("=" * 60)
    print(f"验证市场数据 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    service = InvestmentDataService()
    overview = service.get_market_overview(force_refresh=True)

    failures = 0
    failures += verify_indices(service, overview)
    failures += verify_vix(overview)
    failures += verify_shibor(overview)
    failures += verify_hibor(overview)

    print("\n=== 结论 ===")
    if failures:
        print(f"存在 {failures} 项校验失败")
        raise SystemExit(1)

    print("全部校验通过")


if __name__ == "__main__":
    main()
