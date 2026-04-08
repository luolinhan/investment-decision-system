"""
多因子与结构信号
"""
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


def enrich_price_features(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    if data.empty:
        return data

    for column in ("open", "high", "low", "close", "volume"):
        if column not in data.columns:
            data[column] = pd.NA
        data[column] = pd.to_numeric(data[column], errors="coerce")

    data["close_prev"] = data["close"].shift(1)
    data["price_delta"] = data["close"] - data["close_prev"]
    data["force_index"] = data["price_delta"].fillna(0) * data["volume"].fillna(0)
    data["efi_13"] = data["force_index"].ewm(span=13, adjust=False).mean()
    data["ret_1"] = data["close"].pct_change(1) * 100
    data["ret_3"] = data["close"].pct_change(3) * 100
    data["ma_10"] = data["close"].rolling(10).mean()
    data["ma_20"] = data["close"].rolling(20).mean()
    data["ma_60"] = data["close"].rolling(60).mean()
    data["vol_ma_20"] = data["volume"].rolling(20).mean()
    data["vol_ratio"] = data["volume"] / data["vol_ma_20"].where(data["vol_ma_20"] != 0)
    data["ret_5"] = data["close"].pct_change(5) * 100
    data["ret_20"] = data["close"].pct_change(20) * 100
    data["close_vs_ma_20"] = (data["close"] / data["ma_20"].where(data["ma_20"] != 0) - 1) * 100
    data["close_vs_ma_60"] = (data["close"] / data["ma_60"].where(data["ma_60"] != 0) - 1) * 100
    data["ma_20_slope"] = data["ma_20"].diff(5)
    data["ma_60_slope"] = data["ma_60"].diff(10)
    data["true_range"] = _true_range(data)
    data["atr_14"] = data["true_range"].rolling(14).mean()
    data["atr_pct_14"] = (data["atr_14"] / data["close"].where(data["close"] != 0)) * 100
    data["fractal_top"] = _mark_fractal_top(data)
    data["fractal_bottom"] = _mark_fractal_bottom(data)
    return data


def _true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    ranges = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    )
    return ranges.max(axis=1)


def _mark_fractal_top(df: pd.DataFrame) -> pd.Series:
    high = df["high"]
    return (
        (high > high.shift(1))
        & (high > high.shift(2))
        & (high >= high.shift(-1))
        & (high >= high.shift(-2))
    ).fillna(False)


def _mark_fractal_bottom(df: pd.DataFrame) -> pd.Series:
    low = df["low"]
    return (
        (low < low.shift(1))
        & (low < low.shift(2))
        & (low <= low.shift(-1))
        & (low <= low.shift(-2))
    ).fillna(False)


def analyze_structure(df: pd.DataFrame) -> Dict[str, Any]:
    if len(df) < 30:
        return {
            "label": "insufficient",
            "score": 0,
            "center_low": None,
            "center_high": None,
            "confidence": 0.0,
            "swing_count": 0,
            "risk_flags": ["样本不足"],
            "reasons": ["样本不足，无法判断结构"],
        }

    swings = _extract_swings(df)
    if len(swings) < 4:
        return {
            "label": "no_clear_structure",
            "score": 0,
            "center_low": None,
            "center_high": None,
            "confidence": 0.1,
            "swing_count": len(swings),
            "risk_flags": ["结构样本不足"],
            "reasons": ["近期分型不足，结构不清晰"],
        }

    recent_swings = swings[-5:]
    strokes = []
    for start, end in zip(recent_swings[:-1], recent_swings[1:]):
        if not pd.notna(start["price"]) or not pd.notna(end["price"]):
            continue
        strokes.append(
            {
                "low": min(start["price"], end["price"]),
                "high": max(start["price"], end["price"]),
            }
        )

    if len(strokes) < 3:
        return {
            "label": "no_clear_structure",
            "score": 0,
            "center_low": None,
            "center_high": None,
            "confidence": 0.2,
            "swing_count": len(swings),
            "risk_flags": ["结构笔段不足"],
            "reasons": ["结构笔段不足，暂时无法形成稳定中枢"],
        }

    center_low = max(item["low"] for item in strokes[-3:])
    center_high = min(item["high"] for item in strokes[-3:])
    latest_close = df["close"].iloc[-1]
    if not pd.notna(latest_close):
        return {
            "label": "insufficient",
            "score": 0,
            "center_low": None,
            "center_high": None,
            "confidence": 0.0,
            "swing_count": len(swings),
            "risk_flags": ["收盘价缺失"],
            "reasons": ["收盘价缺失，无法判断结构"],
        }
    latest_close = float(latest_close)
    recent_low_value = df["low"].tail(5).min()
    recent_low = float(recent_low_value) if pd.notna(recent_low_value) else latest_close
    ma_20 = float(df["ma_20"].iloc[-1]) if pd.notna(df["ma_20"].iloc[-1]) else None
    efi_13 = float(df["efi_13"].iloc[-1]) if pd.notna(df["efi_13"].iloc[-1]) else None
    reasons: List[str] = []
    confidence = 0.25 + min(len(swings), 8) * 0.05

    if center_low >= center_high:
        if ma_20 is not None and latest_close > ma_20 and efi_13 is not None and efi_13 > 0:
            return {
                "label": "trend_without_center",
                "score": 7,
                "center_low": None,
                "center_high": None,
                "confidence": min(confidence, 0.7),
                "swing_count": len(swings),
                "risk_flags": ["尚未形成稳定中枢"],
                "reasons": ["未形成有效中枢，但价格仍位于中期均线之上且量能未明显转弱"],
            }
        return {
            "label": "structure_weak",
            "score": 0,
            "center_low": None,
            "center_high": None,
            "confidence": min(confidence, 0.4),
            "swing_count": len(swings),
            "risk_flags": ["未形成有效中枢"],
            "reasons": ["近期没有形成有效中枢重叠，结构性优势不足"],
        }

    center_width = center_high - center_low
    center_width_pct = (center_width / latest_close * 100) if latest_close else 0

    if (
        latest_close > center_high
        and recent_low > center_low
        and ma_20 is not None
        and latest_close >= ma_20
        and efi_13 is not None
        and efi_13 >= 0
    ):
        reasons.append("价格回踩后仍站在中枢上沿之上")
        reasons.append(f"中枢区间约 {center_width_pct:.1f}%")
        return {
            "label": "second_buy",
            "score": 18,
            "center_low": round(center_low, 2),
            "center_high": round(center_high, 2),
            "confidence": min(confidence + 0.25, 0.95),
            "swing_count": len(swings),
            "risk_flags": [],
            "reasons": reasons,
        }

    if latest_close > center_low and latest_close >= (center_high * 0.98):
        reasons.append("价格位于中枢上半区，等待确认突破")
        if center_width_pct < 1.0:
            reasons.append("中枢过窄，信号质量一般")
        return {
            "label": "center_break_attempt",
            "score": 11 if ma_20 is not None and latest_close >= ma_20 else 8,
            "center_low": round(center_low, 2),
            "center_high": round(center_high, 2),
            "confidence": min(confidence + 0.15, 0.8),
            "swing_count": len(swings),
            "risk_flags": ["仍需确认有效突破"] if center_width_pct < 1.0 else [],
            "reasons": reasons,
        }

    reasons.append("价格跌回中枢下沿下方，结构优势减弱")
    risk_flags = ["跌回中枢下沿下方"]
    if efi_13 is not None and efi_13 < 0:
        reasons.append("EFI 仍处于零轴下方，买盘接力不足")
        risk_flags.append("EFI 仍在零轴下方")
    return {
        "label": "structure_weak",
        "score": 0,
        "center_low": round(center_low, 2),
        "center_high": round(center_high, 2),
        "confidence": min(confidence, 0.5),
        "swing_count": len(swings),
        "risk_flags": risk_flags,
        "reasons": reasons,
    }


def _extract_swings(df: pd.DataFrame) -> List[Dict[str, Any]]:
    swings: List[Dict[str, Any]] = []
    view = df.reset_index(drop=True)

    for idx, row in view.iterrows():
        high = row.get("high")
        low = row.get("low")
        if bool(row.get("fractal_top")) and pd.notna(high):
            swings.append({"kind": "top", "price": float(high), "idx": idx})
        if bool(row.get("fractal_bottom")) and pd.notna(low):
            swings.append({"kind": "bottom", "price": float(low), "idx": idx})

    swings.sort(key=lambda item: item["idx"])
    normalized: List[Dict[str, Any]] = []
    for swing in swings:
        if not normalized:
            normalized.append(swing)
            continue

        last = normalized[-1]
        if swing["kind"] == last["kind"]:
            if swing["kind"] == "top" and swing["price"] > last["price"]:
                normalized[-1] = swing
            elif swing["kind"] == "bottom" and swing["price"] < last["price"]:
                normalized[-1] = swing
            continue

        if swing["idx"] - last["idx"] < 3:
            continue
        normalized.append(swing)

    return normalized


def score_trend(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty or len(df) < 20:
        return {
            "score": 0,
            "confidence": 0.0,
            "reasons": ["样本不足，趋势信号不完整"],
            "risk_flags": ["样本不足"],
        }

    latest = df.iloc[-1]
    reasons: List[str] = []
    risk_flags: List[str] = []
    score = 0

    ma_20 = latest.get("ma_20")
    ma_60 = latest.get("ma_60")
    close = latest.get("close")
    efi_13 = latest.get("efi_13")
    vol_ratio = latest.get("vol_ratio")
    ret_5 = latest.get("ret_5")
    ret_20 = latest.get("ret_20")
    close_vs_ma_20 = latest.get("close_vs_ma_20")
    ma_20_slope = latest.get("ma_20_slope")
    ma_60_slope = latest.get("ma_60_slope")
    atr_pct_14 = latest.get("atr_pct_14")

    if pd.notna(ma_20) and pd.notna(ma_60) and ma_20 > ma_60:
        score += 10
        reasons.append("20 日均线位于 60 日均线之上")
    elif pd.notna(ma_20) and pd.notna(ma_60):
        risk_flags.append("中期均线仍未转强")
        if ma_20 < ma_60:
            score -= 3
    if pd.notna(close) and pd.notna(ma_20) and close > ma_20:
        score += 8
        reasons.append("价格运行在 20 日均线之上")
    elif pd.notna(close) and pd.notna(ma_20):
        score -= 4
        risk_flags.append("价格跌破 20 日均线")
    if pd.notna(ma_20_slope) and ma_20_slope > 0:
        score += 3
        reasons.append("20 日均线抬升")
    if pd.notna(ma_60_slope) and ma_60_slope > 0:
        score += 2
        reasons.append("60 日均线走平后转升")
    if pd.notna(efi_13) and efi_13 > 0:
        score += 10
        reasons.append("EFI 13 维持正值")
    if len(df) >= 2 and pd.notna(df["efi_13"].iloc[-2]) and df["efi_13"].iloc[-2] <= 0 < efi_13:
        score += 6
        reasons.append("EFI 13 刚从零轴下方翻正")
    if pd.notna(vol_ratio) and 1.05 <= vol_ratio <= 2.2:
        score += 5
        reasons.append("成交量较 20 日均量放大")
    elif pd.notna(vol_ratio) and vol_ratio < 0.75:
        risk_flags.append("成交量偏弱")
    if pd.notna(ret_5) and ret_5 > 0:
        score += 2
        reasons.append("近 5 日收益为正")
    elif pd.notna(ret_5) and ret_5 < -5:
        risk_flags.append("近 5 日回撤偏大")
        score -= 3
    if pd.notna(ret_20) and ret_20 > 0:
        score += 2
        reasons.append("近 20 日收益为正")
    elif pd.notna(ret_20) and ret_20 < -10:
        risk_flags.append("近 20 日趋势仍偏弱")
        score -= 4
    if pd.notna(close_vs_ma_20) and close_vs_ma_20 > 15:
        risk_flags.append("短线涨幅偏快，存在回吐风险")
        score -= 2
    if pd.notna(atr_pct_14) and atr_pct_14 > 8:
        risk_flags.append("波动率偏高")

    confidence = 0.25
    if pd.notna(ma_20) and pd.notna(ma_60):
        confidence += 0.2
    if pd.notna(efi_13):
        confidence += 0.2
    if pd.notna(ret_20):
        confidence += 0.15
    if pd.notna(vol_ratio):
        confidence += 0.1
    if len(df) >= 60:
        confidence += 0.1

    return {
        "score": max(score, 0),
        "confidence": min(confidence, 1.0),
        "reasons": reasons,
        "risk_flags": risk_flags,
    }
