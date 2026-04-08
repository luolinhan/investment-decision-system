"""扫描数据库结构"""
import sqlite3
import json

conn = sqlite3.connect("C:/Users/Administrator/research_report_system/data/investment.db")
c = conn.cursor()

# 获取所有表
c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in c.fetchall()]

result = {}
for t in tables:
    c.execute(f"PRAGMA table_info({t})")
    cols = [{"name": r[1], "type": r[2]} for r in c.fetchall()]
    c.execute(f"SELECT COUNT(*) FROM {t}")
    count = c.fetchone()[0]
    result[t] = {"columns": cols, "row_count": count}

print(json.dumps(result, indent=2, ensure_ascii=False))
conn.close()