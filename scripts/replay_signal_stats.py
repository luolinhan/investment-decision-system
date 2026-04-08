"""
信号统计回放脚本
按setup/regime/score_bucket统计历史表现
"""
import sqlite3
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "investment.db"
OUTPUT_DIR = BASE_DIR / "data" / "stats"


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def analyze_by_setup(c) -> Dict:
    """按setup类型统计"""
    c.execute("""
        SELECT
            setup_type,
            COUNT(*) as sample_size,
            AVG(CASE WHEN t5_ret IS NOT NULL THEN 1.0 ELSE 0 END) as coverage,
            AVG(t5_ret) as avg_t5_ret,
            AVG(CASE WHEN t5_ret > 0 THEN 1.0 ELSE 0 END) as win_rate,
            AVG(max_gain) as avg_max_gain,
            AVG(max_drawdown) as avg_max_dd
        FROM strategy_signals_v2 s
        LEFT JOIN signal_labels l ON s.symbol = l.code AND s.as_of_date = l.signal_date
        WHERE s.eligibility_pass = 1
        GROUP BY setup_type
        ORDER BY sample_size DESC
    """)

    results = {}
    for row in c.fetchall():
        results[row["setup_type"]] = {
            "sample_size": row["sample_size"],
            "coverage": round(row["coverage"] * 100, 1) if row["coverage"] else 0,
            "avg_t5_ret": round(row["avg_t5_ret"], 2) if row["avg_t5_ret"] else None,
            "win_rate": round(row["win_rate"] * 100, 1) if row["win_rate"] else None,
            "avg_max_gain": round(row["avg_max_gain"], 2) if row["avg_max_gain"] else None,
            "avg_max_dd": round(row["avg_max_dd"], 2) if row["avg_max_dd"] else None,
        }

    return results


def analyze_by_regime(c) -> Dict:
    """按regime统计"""
    c.execute("""
        SELECT
            regime,
            COUNT(*) as sample_size,
            AVG(t5_ret) as avg_t5_ret,
            AVG(CASE WHEN t5_ret > 0 THEN 1.0 ELSE 0 END) as win_rate
        FROM strategy_signals_v2 s
        LEFT JOIN signal_labels l ON s.symbol = l.code AND s.as_of_date = l.signal_date
        WHERE s.eligibility_pass = 1
        GROUP BY regime
        ORDER BY sample_size DESC
    """)

    results = {}
    for row in c.fetchall():
        results[row["regime"]] = {
            "sample_size": row["sample_size"],
            "avg_t5_ret": round(row["avg_t5_ret"], 2) if row["avg_t5_ret"] else None,
            "win_rate": round(row["win_rate"] * 100, 1) if row["win_rate"] else None,
        }

    return results


def analyze_by_score_bucket(c) -> Dict:
    """按分数区间统计"""
    buckets = [
        ("90-100", 90, 100),
        ("80-90", 80, 90),
        ("70-80", 70, 80),
        ("60-70", 60, 70),
        ("50-60", 50, 60),
        ("<50", 0, 50),
    ]

    results = {}
    for bucket_name, min_score, max_score in buckets:
        c.execute("""
            SELECT
                COUNT(*) as sample_size,
                AVG(t5_ret) as avg_t5_ret,
                AVG(CASE WHEN t5_ret > 0 THEN 1.0 ELSE 0 END) as win_rate,
                AVG(max_gain) as avg_max_gain,
                AVG(max_drawdown) as avg_max_dd
            FROM strategy_signals_v2 s
            LEFT JOIN signal_labels l ON s.symbol = l.code AND s.as_of_date = l.signal_date
            WHERE s.eligibility_pass = 1
              AND s.score_total >= ? AND s.score_total < ?
        """, (min_score, max_score))

        row = c.fetchone()
        if row and row["sample_size"] > 0:
            results[bucket_name] = {
                "score_range": f"{min_score}-{max_score}",
                "sample_size": row["sample_size"],
                "avg_t5_ret": round(row["avg_t5_ret"], 2) if row["avg_t5_ret"] else None,
                "win_rate": round(row["win_rate"] * 100, 1) if row["win_rate"] else None,
                "avg_max_gain": round(row["avg_max_gain"], 2) if row["avg_max_gain"] else None,
                "avg_max_dd": round(row["avg_max_dd"], 2) if row["avg_max_dd"] else None,
            }

    return results


def analyze_by_grade(c) -> Dict:
    """按Grade统计"""
    c.execute("""
        SELECT
            grade,
            COUNT(*) as sample_size,
            AVG(score_total) as avg_score,
            AVG(t5_ret) as avg_t5_ret,
            AVG(CASE WHEN t5_ret > 0 THEN 1.0 ELSE 0 END) as win_rate,
            AVG(max_gain) as avg_max_gain,
            AVG(max_drawdown) as avg_max_dd
        FROM strategy_signals_v2 s
        LEFT JOIN signal_labels l ON s.symbol = l.code AND s.as_of_date = l.signal_date
        GROUP BY grade
        ORDER BY
            CASE grade
                WHEN 'A' THEN 1
                WHEN 'B' THEN 2
                WHEN 'C' THEN 3
                ELSE 4
            END
    """)

    results = {}
    for row in c.fetchall():
        results[row["grade"]] = {
            "sample_size": row["sample_size"],
            "avg_score": round(row["avg_score"], 1) if row["avg_score"] else None,
            "avg_t5_ret": round(row["avg_t5_ret"], 2) if row["avg_t5_ret"] else None,
            "win_rate": round(row["win_rate"] * 100, 1) if row["win_rate"] else None,
            "avg_max_gain": round(row["avg_max_gain"], 2) if row["avg_max_gain"] else None,
            "avg_max_dd": round(row["avg_max_dd"], 2) if row["avg_max_dd"] else None,
        }

    return results


def analyze_setup_regime_matrix(c) -> Dict:
    """Setup x Regime交叉分析"""
    c.execute("""
        SELECT
            setup_type,
            regime,
            COUNT(*) as sample_size,
            AVG(t5_ret) as avg_t5_ret,
            AVG(CASE WHEN t5_ret > 0 THEN 1.0 ELSE 0 END) as win_rate
        FROM strategy_signals_v2 s
        LEFT JOIN signal_labels l ON s.symbol = l.code AND s.as_of_date = l.signal_date
        WHERE s.eligibility_pass = 1
        GROUP BY setup_type, regime
        HAVING sample_size >= 5
        ORDER BY setup_type, regime
    """)

    matrix = defaultdict(dict)
    for row in c.fetchall():
        matrix[row["setup_type"]][row["regime"]] = {
            "sample_size": row["sample_size"],
            "avg_t5_ret": round(row["avg_t5_ret"], 2) if row["avg_t5_ret"] else None,
            "win_rate": round(row["win_rate"] * 100, 1) if row["win_rate"] else None,
        }

    return dict(matrix)


def analyze_filter_effectiveness(c) -> Dict:
    """过滤器有效性分析"""
    c.execute("""
        SELECT
            eligibility_pass,
            COUNT(*) as sample_size,
            AVG(t5_ret) as avg_t5_ret,
            AVG(CASE WHEN t5_ret > 0 THEN 1.0 ELSE 0 END) as win_rate
        FROM strategy_signals_v2 s
        LEFT JOIN signal_labels l ON s.symbol = l.code AND s.as_of_date = l.signal_date
        GROUP BY eligibility_pass
    """)

    results = {}
    for row in c.fetchall():
        key = "passed" if row["eligibility_pass"] else "filtered_out"
        results[key] = {
            "sample_size": row["sample_size"],
            "avg_t5_ret": round(row["avg_t5_ret"], 2) if row["avg_t5_ret"] else None,
            "win_rate": round(row["win_rate"] * 100, 1) if row["win_rate"] else None,
        }

    return results


def calculate_calibration_params(stats: Dict) -> Dict:
    """计算校准参数"""
    calibration = {}

    # Setup校准
    setup_stats = stats.get("by_setup", {})
    for setup_type, data in setup_stats.items():
        if data["sample_size"] >= 30 and data["win_rate"]:
            # 基于历史胜率调整建议权重
            win_rate = data["win_rate"]
            if win_rate >= 60:
                recommended_weight = 1.2
                confidence = "high"
            elif win_rate >= 55:
                recommended_weight = 1.0
                confidence = "medium"
            else:
                recommended_weight = 0.8
                confidence = "low"

            calibration[setup_type] = {
                "recommended_weight": recommended_weight,
                "confidence": confidence,
                "historical_win_rate": win_rate,
                "sample_size": data["sample_size"],
            }

    return calibration


def run_replay(output_format: str = "json") -> Dict:
    """运行完整回放分析"""
    conn = get_db_connection()
    c = conn.cursor()

    stats = {
        "generated_at": datetime.now().isoformat(),
        "by_setup": analyze_by_setup(c),
        "by_regime": analyze_by_regime(c),
        "by_score_bucket": analyze_by_score_bucket(c),
        "by_grade": analyze_by_grade(c),
        "setup_regime_matrix": analyze_setup_regime_matrix(c),
        "filter_effectiveness": analyze_filter_effectiveness(c),
    }

    # 计算校准参数
    stats["calibration"] = calculate_calibration_params(stats)

    conn.close()

    # 保存结果
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if output_format in ("json", "all"):
        output_path = OUTPUT_DIR / f"signal_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        print(f"统计结果已保存: {output_path}")

    if output_format in ("csv", "all"):
        import csv

        # 保存主要统计
        csv_path = OUTPUT_DIR / f"signal_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["category", "key", "sample_size", "win_rate", "avg_t5_ret", "avg_max_gain", "avg_max_dd"])

            for setup, data in stats["by_setup"].items():
                writer.writerow(["setup", setup, data["sample_size"], data["win_rate"],
                                data["avg_t5_ret"], data["avg_max_gain"], data["avg_max_dd"]])

            for grade, data in stats["by_grade"].items():
                writer.writerow(["grade", grade, data["sample_size"], data["win_rate"],
                                data["avg_t5_ret"], data["avg_max_gain"], data["avg_max_dd"]])

            for bucket, data in stats["by_score_bucket"].items():
                writer.writerow(["score_bucket", bucket, data["sample_size"], data["win_rate"],
                                data["avg_t5_ret"], data["avg_max_gain"], data["avg_max_dd"]])

        print(f"CSV已保存: {csv_path}")

    return stats


def print_summary(stats: Dict):
    """打印摘要"""
    print("\n" + "=" * 60)
    print("信号统计摘要")
    print("=" * 60)

    print("\n[按Grade统计]")
    for grade, data in stats.get("by_grade", {}).items():
        print(f"  {grade}: n={data['sample_size']}, 胜率={data['win_rate']}%, "
              f"5日收益={data['avg_t5_ret']}%")

    print("\n[按Setup统计]")
    for setup, data in stats.get("by_setup", {}).items():
        print(f"  {setup}: n={data['sample_size']}, 胜率={data['win_rate']}%")

    print("\n[按分数区间统计]")
    for bucket, data in stats.get("by_score_bucket", {}).items():
        print(f"  {bucket}: n={data['sample_size']}, 胜率={data['win_rate']}%")

    print("\n[过滤器有效性]")
    fe = stats.get("filter_effectiveness", {})
    for key, data in fe.items():
        print(f"  {key}: n={data['sample_size']}, 胜率={data['win_rate']}%")

    print("\n[校准建议]")
    for setup, cal in stats.get("calibration", {}).items():
        print(f"  {setup}: 权重={cal['recommended_weight']}, "
              f"置信度={cal['confidence']}, 历史胜率={cal['historical_win_rate']}%")


if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("信号统计回放")
    print("=" * 60)

    output_format = "all"
    if "--json" in sys.argv:
        output_format = "json"
    elif "--csv" in sys.argv:
        output_format = "csv"

    stats = run_replay(output_format=output_format)
    print_summary(stats)

    print("\n完成!")