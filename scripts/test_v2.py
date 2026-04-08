"""测试V2路由"""
import sys
import sqlite3
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# 测试数据库
db_path = Path("data/investment.db")
print(f"DB exists: {db_path.exists()}")
print(f"DB absolute: {db_path.resolve()}")

conn = sqlite3.connect(str(db_path))
c = conn.cursor()
c.execute("SELECT COUNT(*) FROM strategy_signals_v2")
print(f"Signals count: {c.fetchone()[0]}")
conn.close()

# 测试路由导入
from app.routers.investment_v2 import router_v2
print("Router loaded OK")

# 测试服务导入
from app.services.strategy_v2_service import StrategyV2Service
print("Strategy service loaded OK")

# 测试具体函数
from datetime import datetime
date_filter = datetime.now().strftime("%Y-%m-%d")
print(f"Date filter: {date_filter}")

# 测试查询
conn = sqlite3.connect(str(db_path))
conn.row_factory = sqlite3.Row
c = conn.cursor()
c.execute("""
    SELECT signal_id, symbol, grade, score_total FROM strategy_signals_v2
    WHERE as_of_date = ? AND eligibility_pass = 1
    ORDER BY score_total DESC LIMIT 3
""", (date_filter,))
rows = c.fetchall()
print(f"Found {len(rows)} signals for today")
for row in rows:
    print(f"  {row['symbol']}: grade={row['grade']}, score={row['score_total']}")
conn.close()