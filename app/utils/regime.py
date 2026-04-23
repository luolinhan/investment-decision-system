"""
市场状态 (Regime) 计算 - 统一入口

decision-center 和 macro/overview 共用此函数，
避免两处实现不一致导致返回不同状态。
"""


def compute_regime(
    breadth_ratio: float,
    limit_ratio: float,
    north_flow_impulse: float,
    vix_close: float,
    error_count: int = 0,
) -> dict:
    """
    计算市场状态。

    评分规则 (每项 ±1 或 0):
    - 广度: 涨跌比 ≥1.1 加 1 分, ≤0.9 扣 1 分
    - 涨停结构: 涨停/跌停 ≥1.5 加 1 分, ≤0.8 扣 1 分
    - 资金动量: 5日相对20日基线 ≥50 加 1 分, ≤-50 扣 1 分
    - 波动率: VIX ≤16 加 1 分, ≥24 扣 1 分
    - 数据健康: 错误数 >0 扣 1 分

    结果:
    - score ≥ 2 → risk_on
    - score ≤ -2 → risk_off
    - 其他 → neutral
    """
    score = 0
    score += 1 if breadth_ratio >= 1.1 else -1 if breadth_ratio <= 0.9 else 0
    score += 1 if limit_ratio >= 1.5 else -1 if limit_ratio <= 0.8 else 0
    score += 1 if north_flow_impulse >= 50 else -1 if north_flow_impulse <= -50 else 0
    score += 1 if vix_close <= 16 else -1 if vix_close >= 24 else 0
    if error_count > 0:
        score -= 1

    if score >= 2:
        label = "risk_on"
    elif score <= -2:
        label = "risk_off"
    else:
        label = "neutral"

    return {"label": label, "score": score}
