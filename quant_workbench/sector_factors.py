"""
板块因子计算

提供板块动量排名、轮动信号、龙头识别等量化因子。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def calc_momentum_rank(
    sector_data: List[Dict[str, Any]],
    window: str = "1d",
) -> List[Dict[str, Any]]:
    """
    板块动量排名

    参数:
        sector_data: 板块列表，每项包含 sector_name, change_pct, turnover 等
        window: 时间窗口 ("1d", "3d", "5d")

    返回:
        按动量排序的板块列表，增加 momentum_score 和 momentum_rank
    """
    ranked = []
    for s in sector_data:
        change_pct = s.get("change_pct") or 0.0
        turnover = s.get("turnover") or 0.0

        # 动量分数 = 涨跌幅 * 0.7 + 成交额对数 * 0.3
        import math
        turnover_score = math.log1p(turnover) * 0.3
        momentum_score = change_pct * 0.7 + turnover_score

        ranked.append({
            **s,
            "momentum_score": round(momentum_score, 2),
        })

    ranked.sort(key=lambda x: x["momentum_score"], reverse=True)

    for i, item in enumerate(ranked):
        item["momentum_rank"] = i + 1

    return ranked


def rotation_signal(
    current_sectors: List[Dict[str, Any]],
    prev_sectors: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    板块轮动信号

    识别资金从哪些板块流出、流入哪些板块。
    如果提供了历史板块数据，比较排名变化。

    返回:
        {
            "hot_sectors": [...],       # 资金加速流入
            "cold_sectors": [...],      # 资金加速流出
            "rotation_direction": str,   # 轮动方向描述
            "breadth_ratio": float,      # 涨跌比
        }
    """
    if not current_sectors:
        return {
            "hot_sectors": [],
            "cold_sectors": [],
            "rotation_direction": "数据不足",
            "breadth_ratio": 0.0,
        }

    rising = [s for s in current_sectors if (s.get("change_pct") or 0) > 0]
    falling = [s for s in current_sectors if (s.get("change_pct") or 0) <= 0]
    breadth_ratio = len(rising) / max(len(falling), 1)

    # 按涨跌幅排序
    sorted_by_change = sorted(
        current_sectors,
        key=lambda x: x.get("change_pct") or 0,
        reverse=True
    )

    hot_sectors = sorted_by_change[:10]
    cold_sectors = sorted_by_change[-10:]

    # 轮动方向描述
    hot_names = [s.get("sector_name", "") for s in hot_sectors if s.get("change_pct", 0) > 0]
    cold_names = [s.get("sector_name", "") for s in cold_sectors if s.get("change_pct", 0) < 0]

    direction_parts = []
    if hot_names:
        direction_parts.append(f"资金偏多: {', '.join(hot_names[:5])}")
    if cold_names:
        direction_parts.append(f"资金偏空: {', '.join(cold_names[:5])}")

    rotation_direction = "; ".join(direction_parts) if direction_parts else "无明显轮动"

    return {
        "hot_sectors": hot_sectors,
        "cold_sectors": cold_sectors,
        "rotation_direction": rotation_direction,
        "breadth_ratio": round(breadth_ratio, 2),
        "rising_count": len(rising),
        "falling_count": len(falling),
    }


def identify_leader(
    sector_stocks: List[Dict[str, Any]],
    top_n: int = 3,
) -> List[Dict[str, Any]]:
    """
    板块内龙头识别

    综合考量:
    - 涨跌幅 (40%)
    - 成交额 (30%)
    - 换手活跃度 (30%)

    返回:
        板块内龙头股排行
    """
    if not sector_stocks:
        return []

    scored = []
    for s in sector_stocks:
        change_pct = s.get("change_pct") or 0.0
        turnover = s.get("turnover") or 0.0
        volume = s.get("volume") or 0.0

        # 成交额排名归一化
        import math
        turnover_log = math.log1p(turnover)
        volume_log = math.log1p(volume)

        # 综合评分
        leader_score = (
            change_pct * 0.4
            + turnover_log * 0.3
            + volume_log * 0.3
        )

        scored.append({
            **s,
            "leader_score": round(leader_score, 2),
        })

    scored.sort(key=lambda x: x["leader_score"], reverse=True)
    return scored[:top_n]


def sector_valuation_percentile(
    current_pe: Optional[float],
    hist_pe: Optional[List[float]] = None,
) -> Dict[str, Any]:
    """
    板块估值历史分位

    如果提供了历史PE数据，计算当前PE的分位排名。
    """
    if current_pe is None or not hist_pe:
        return {
            "current_pe": current_pe,
            "percentile_5y": None,
            "status": "insufficient_data",
        }

    sorted_hist = sorted(hist_pe)
    n = len(sorted_hist)
    count_below = sum(1 for v in sorted_hist if v < current_pe)
    percentile = count_below / n * 100 if n > 0 else None

    if percentile is None:
        status = "unknown"
    elif percentile < 20:
        status = "undervalued"
    elif percentile < 40:
        status = "fair_value"
    elif percentile < 60:
        status = "reasonable"
    elif percentile < 80:
        status = "overvalued"
    else:
        status = "bubble"

    return {
        "current_pe": current_pe,
        "percentile_5y": round(percentile, 1) if percentile else None,
        "median_pe": round(sorted_hist[n // 2], 2) if sorted_hist else None,
        "status": status,
    }
