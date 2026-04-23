"""
动态市场扫描：生成 A 股 + 港股代码清单，并过滤高风险标的（ST/退市等）

支持:
- A 股: 沪深300 + 中证500 成分股 (akshare index_stock_cons)
- 港股: 热门港股排行 (akshare stock_hk_hot_rank_em)
"""
from __future__ import annotations

from typing import List

import akshare as ak

from quant_workbench.universe import Instrument


def _is_low_risk(name: str) -> bool:
    """过滤 ST、*ST、退市、N/A 等高风险标的名称。"""
    if not name:
        return False
    bad_flags = ["ST", "*ST", "退", "PT", "摘牌"]
    upper = name.upper()
    return not any(flag in upper for flag in bad_flags)


def _a_share_to_yahoo(code: str) -> str:
    if code.startswith("6"):
        return f"{code}.SS"
    return f"{code}.SZ"


def _a_share_to_code(code: str) -> str:
    if code.startswith("6"):
        return f"sh{code}"
    return f"sz{code}"


def _hk_code_to_instrument(code: str, name: str) -> Instrument:
    """将港股代码转换为 Instrument。code 格式如 00700。"""
    padded = code.zfill(5)
    return Instrument(
        code=f"hk{padded}",
        finance_code=f"hk{padded}",
        report_code=f"{padded}.HK",
        name=name,
        market="HK",
        category="Dynamic",
        yahoo_symbol=f"{padded}.HK",
        aliases=[name],
    )


def load_a_share_universe(limit: int = 800) -> List[Instrument]:
    """
    拉取 A 股成分股列表，过滤高风险标的。
    组合沪深300 + 中证500，去重后取前 limit。
    """
    instruments: List[Instrument] = []
    seen = set()

    for index_code in ["000300", "000905"]:
        try:
            df = ak.index_stock_cons(symbol=index_code)
            if df is None or df.empty:
                continue
        except Exception as exc:
            print(f"获取 {index_code} 成分股失败: {exc}")
            continue

        # 过滤高风险
        name_col = "品种名称" if "品种名称" in df.columns else "name"
        code_col = "品种代码" if "品种代码" in df.columns else "code"

        df = df[df[name_col].apply(_is_low_risk)]

        for _, row in df.iterrows():
            code = str(row[code_col]).zfill(6)
            name = str(row[name_col])
            if code in seen:
                continue
            seen.add(code)

            instruments.append(
                Instrument(
                    code=_a_share_to_code(code),
                    finance_code=code,
                    report_code=f"{code}.{'SH' if code.startswith('6') else 'SZ'}",
                    name=name,
                    market="A",
                    category="Index",
                    yahoo_symbol=_a_share_to_yahoo(code),
                    aliases=[name],
                )
            )
            if len(instruments) >= limit:
                break

    return instruments


def load_hk_universe(limit: int = 60) -> List[Instrument]:
    """
    拉取港股热门排行，过滤后返回 Instrument 列表。
    """
    try:
        df = ak.stock_hk_hot_rank_em()
        if df is None or df.empty:
            return []
    except Exception as exc:
        print(f"获取港股热榜失败: {exc}")
        return []

    instruments: List[Instrument] = []
    for _, row in df.iterrows():
        code = str(row.get("代码", ""))
        name = str(row.get("股票名称", ""))
        if not code or not name:
            continue
        if not _is_low_risk(name):
            continue

        instruments.append(_hk_code_to_instrument(code, name))
        if len(instruments) >= limit:
            break

    return instruments
