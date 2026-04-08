"""
策略系统V2迁移脚本
新增表和字段，保持幂等和向后兼容
"""
import sqlite3
import json
from datetime import datetime
from pathlib import Path

# 脚本在scripts/目录下，需要parent.parent回到项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "investment.db"


def backup_database():
    """备份数据库"""
    import shutil
    backup_path = BASE_DIR / "data" / f"investment_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    shutil.copy(DB_PATH, backup_path)
    print(f"数据库已备份到: {backup_path}")
    return backup_path


def run_migration():
    """执行迁移"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    migrations = []

    # 1. 统一信号表 v2 (扩展现有signal_journal，新增字段)
    migrations.append("""
        CREATE TABLE IF NOT EXISTS strategy_signals_v2 (
            signal_id TEXT PRIMARY KEY,
            as_of_date TEXT NOT NULL,
            as_of_ts TEXT,
            symbol TEXT NOT NULL,
            symbol_name TEXT,
            setup_type TEXT,
            setup_subtype TEXT,
            holding_horizon TEXT DEFAULT 'short',
            regime TEXT,

            -- 评分
            score_total REAL DEFAULT 0,
            score_quality REAL DEFAULT 0,
            score_growth REAL DEFAULT 0,
            score_valuation REAL DEFAULT 0,
            score_technical REAL DEFAULT 0,
            score_flow REAL DEFAULT 0,
            score_catalyst REAL DEFAULT 0,
            score_sentiment REAL DEFAULT 0,
            risk_penalty REAL DEFAULT 0,

            -- 过滤结果
            eligibility_pass INTEGER DEFAULT 1,
            filter_fail_reasons TEXT,
            risk_flags TEXT,

            -- 分级
            grade TEXT DEFAULT 'C',
            action TEXT DEFAULT 'SKIP',
            action_reason TEXT,

            -- 数据质量
            data_freshness_hours REAL,
            data_coverage_pct REAL,
            source_confidence REAL DEFAULT 0.5,

            -- 版本
            strategy_version TEXT DEFAULT 'v2.0',
            weights_version TEXT DEFAULT 'default',

            -- 计划字段
            planned_entry_rule TEXT,
            planned_stop_pct REAL,
            planned_take_pct REAL,
            planned_max_position_pct REAL,

            created_at TEXT,
            updated_at TEXT
        )
    """)

    # 索引
    migrations.append("CREATE INDEX IF NOT EXISTS idx_signals_v2_date ON strategy_signals_v2(as_of_date)")
    migrations.append("CREATE INDEX IF NOT EXISTS idx_signals_v2_symbol ON strategy_signals_v2(symbol)")
    migrations.append("CREATE INDEX IF NOT EXISTS idx_signals_v2_setup ON strategy_signals_v2(setup_type)")
    migrations.append("CREATE INDEX IF NOT EXISTS idx_signals_v2_grade ON strategy_signals_v2(grade)")

    # 2. 交易计划表
    migrations.append("""
        CREATE TABLE IF NOT EXISTS trade_plans (
            trade_plan_id TEXT PRIMARY KEY,
            signal_id TEXT,
            plan_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            setup_type TEXT,

            -- 入场规则
            entry_rule TEXT,
            entry_window_start TEXT,
            entry_window_end TEXT,
            max_chase_pct REAL DEFAULT 3.0,

            -- 止盈止损
            stop_loss_pct REAL DEFAULT 5.0,
            take_profit_pct REAL DEFAULT 10.0,
            time_stop_days INTEGER DEFAULT 5,

            -- 仓位
            max_position_pct REAL DEFAULT 5.0,

            -- 作废条件
            invalidation_rule TEXT,
            invalidation_conditions TEXT,

            -- 优先级
            priority_rank INTEGER DEFAULT 5,

            -- 状态
            status TEXT DEFAULT 'pending',

            -- 关联执行
            execution_id TEXT,

            created_at TEXT,
            updated_at TEXT
        )
    """)

    migrations.append("CREATE INDEX IF NOT EXISTS idx_trade_plans_date ON trade_plans(plan_date)")
    migrations.append("CREATE INDEX IF NOT EXISTS idx_trade_plans_signal ON trade_plans(signal_id)")
    migrations.append("CREATE INDEX IF NOT EXISTS idx_trade_plans_status ON trade_plans(status)")

    # 3. 扩展 signal_labels (如果字段不存在则添加)
    try:
        c.execute("SELECT t1_ret FROM signal_labels LIMIT 1")
    except sqlite3.OperationalError:
        # 字段不存在，需要添加
        migrations.append("ALTER TABLE signal_labels ADD COLUMN t1_ret REAL")
        migrations.append("ALTER TABLE signal_labels ADD COLUMN t3_ret REAL")
        migrations.append("ALTER TABLE signal_labels ADD COLUMN t5_ret REAL")
        migrations.append("ALTER TABLE signal_labels ADD COLUMN t10_ret REAL")
        migrations.append("ALTER TABLE signal_labels ADD COLUMN max_upside_5d REAL")
        migrations.append("ALTER TABLE signal_labels ADD COLUMN max_drawdown_5d REAL")
        migrations.append("ALTER TABLE signal_labels ADD COLUMN hit_takeprofit_5d INTEGER")
        migrations.append("ALTER TABLE signal_labels ADD COLUMN hit_stoploss_5d INTEGER")
        migrations.append("ALTER TABLE signal_labels ADD COLUMN alpha_vs_benchmark_5d REAL")
        migrations.append("ALTER TABLE signal_labels ADD COLUMN label_status TEXT DEFAULT 'pending'")
        migrations.append("ALTER TABLE signal_labels ADD COLUMN labeled_at TEXT")

    # 4. 扩展 execution_journal (关联 signal_id)
    try:
        c.execute("SELECT signal_id FROM execution_journal LIMIT 1")
    except sqlite3.OperationalError:
        migrations.append("ALTER TABLE execution_journal ADD COLUMN signal_id TEXT")
        migrations.append("ALTER TABLE execution_journal ADD COLUMN planned_action TEXT")
        migrations.append("ALTER TABLE execution_journal ADD COLUMN executed_flag INTEGER DEFAULT 0")
        migrations.append("ALTER TABLE execution_journal ADD COLUMN not_executed_reason TEXT")
        migrations.append("ALTER TABLE execution_journal ADD COLUMN actual_entry_time TEXT")
        migrations.append("ALTER TABLE execution_journal ADD COLUMN actual_entry_price REAL")
        migrations.append("ALTER TABLE execution_journal ADD COLUMN actual_exit_time TEXT")
        migrations.append("ALTER TABLE execution_journal ADD COLUMN actual_exit_price REAL")
        migrations.append("ALTER TABLE execution_journal ADD COLUMN position_pct REAL")
        migrations.append("ALTER TABLE execution_journal ADD COLUMN slippage_bps REAL")
        migrations.append("ALTER TABLE execution_journal ADD COLUMN cancel_reason TEXT")
        migrations.append("ALTER TABLE execution_journal ADD COLUMN manual_override_flag INTEGER DEFAULT 0")

    # 5. 数据质量门控日志表
    migrations.append("""
        CREATE TABLE IF NOT EXISTS data_quality_gates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gate_date TEXT NOT NULL,
            symbol TEXT,
            gate_type TEXT NOT NULL,
            gate_result TEXT NOT NULL,
            fail_reason TEXT,
            metric_value REAL,
            threshold_value REAL,
            data_age_hours REAL,
            created_at TEXT
        )
    """)

    migrations.append("CREATE INDEX IF NOT EXISTS idx_quality_gates_date ON data_quality_gates(gate_date)")
    migrations.append("CREATE INDEX IF NOT EXISTS idx_quality_gates_symbol ON data_quality_gates(symbol)")

    # 6. Setup校准表 (扩展 strategy_regime_calibration)
    migrations.append("""
        CREATE TABLE IF NOT EXISTS setup_calibration_v2 (
            calibration_id TEXT PRIMARY KEY,
            week_id TEXT NOT NULL,
            regime TEXT NOT NULL,
            setup_type TEXT NOT NULL,

            sample_size INTEGER DEFAULT 0,
            win_rate REAL DEFAULT 0,
            avg_return REAL DEFAULT 0,
            avg_max_drawdown REAL DEFAULT 0,
            profit_factor REAL DEFAULT 0,

            -- 建议参数
            recommended_weight REAL DEFAULT 0,
            min_score_threshold REAL DEFAULT 60,
            max_position_pct REAL DEFAULT 5,
            suggested_stop_pct REAL DEFAULT 5,
            suggested_take_pct REAL DEFAULT 10,

            -- 置信度
            confidence_level TEXT DEFAULT 'low',

            updated_at TEXT
        )
    """)

    migrations.append("CREATE INDEX IF NOT EXISTS idx_calibration_week ON setup_calibration_v2(week_id)")
    migrations.append("CREATE INDEX IF NOT EXISTS idx_calibration_setup ON setup_calibration_v2(setup_type)")

    # 执行迁移
    success_count = 0
    for sql in migrations:
        try:
            c.execute(sql)
            success_count += 1
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower() or "already exists" in str(e).lower():
                success_count += 1  # 忽略已存在的列/表
            else:
                print(f"迁移警告: {e}")

    conn.commit()
    conn.close()

    print(f"迁移完成: {success_count}/{len(migrations)} 条成功")
    return success_count


if __name__ == "__main__":
    print("=" * 60)
    print("策略系统V2迁移")
    print("=" * 60)

    # 备份
    backup_path = backup_database()

    # 执行迁移
    result = run_migration()

    print(f"\n迁移结果: {result}")
    print("建议验证: python -c \"import sqlite3; c=sqlite3.connect('data/investment.db'); print('OK')\"")