"""
动态市场扫描：生成 A 股 + 港股代码清单，并过滤高风险标的（ST/退市等）
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


def load_a_share_universe(limit: int = 400) -> List[Instrument]:
    """拉取 A 股列表，过滤高风险标的，截取前 limit（按市值排序）。"""
    df = ak.stock_info_a_code_name()
    # 尝试按总市值降序，如果没有字段则按代码排序
    if "total_market_cap" in df.columns:
        df = df.sort_values("total_market_cap", ascending=False)
    df = df[df["name"].apply(_is_low_risk)]

    universe: List[Instrument] = []
    for _, row in df.head(limit).iterrows():
        code = str(row["code"]).zfill(6)
        name = str(row["name"])
        universe.append(
            Instrument(
                code=f"sh{code}" if code.startswith("6") else f"sz{code}",
                finance_code=f"{code[:6]}.{'SH' if code.startswith('6') else 'SZ'}",
                report_code=f"{code[:6]}.{'SH' if code.startswith('6') else 'SZ'}",
                name=name,
                market="A",
                category="AutoScan",
                yahoo_symbol=_a_share_to_yahoo(code),
                aliases=[name],
            )
        )
    return universe
