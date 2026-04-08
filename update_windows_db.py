import json
import sqlite3
import sys

data = json.loads(sys.argv[1])
DB = r"C:\Users\Administrator\research_report_system\data\investment.db"

conn = sqlite3.connect(DB)
c = conn.cursor()
today = data["update_time"][:10]

# 清空并更新指数
c.execute("DELETE FROM index_history")
for idx in data["indices"]:
    c.execute("INSERT OR REPLACE INTO index_history (code, name, trade_date, close, change_pct) VALUES (?,?,?,?,?)",
              (idx["code"], idx["name"], today, idx["close"], idx["change_pct"]))
print(f"指数: {len(data['indices'])} 条")

# 清空并更新股票（跳过全为空的占位记录）
c.execute("DELETE FROM stock_financial")
saved = 0
for s in data["stocks"]:
    values = [
        s.get("pe_ttm"),
        s.get("pb"),
        s.get("roe"),
        s.get("gross_margin"),
        s.get("net_margin"),
        s.get("revenue_yoy"),
        s.get("profit_yoy"),
        s.get("dividend_yield"),
    ]
    if all(v in (None, "", "-") for v in values):
        continue
    c.execute(
        '''INSERT INTO stock_financial 
        (code, name, report_date, pe_ttm, pb, roe, gross_margin, net_margin, revenue_yoy, net_profit_yoy, dividend_yield)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (
            s["code"],
            s["name"],
            today,
            s.get("pe_ttm"),
            s.get("pb"),
            s.get("roe"),
            s.get("gross_margin"),
            s.get("net_margin"),
            s.get("revenue_yoy"),
            s.get("profit_yoy"),
            s.get("dividend_yield"),
        ),
    )
    saved += 1
print(f"股票: {saved} 条")

# VIX
c.execute("DELETE FROM vix_history")
c.execute("INSERT INTO vix_history (trade_date, vix_close) VALUES (?,?)", (today, data["vix"]["close"]))
print(f"VIX: {data['vix']['close']}")

# 利率
c.execute("DELETE FROM interest_rates")
r = data["rates"]
c.execute("INSERT INTO interest_rates (trade_date, shibor_overnight, shibor_1w, shibor_1m, shibor_3m, shibor_6m, shibor_1y) VALUES (?,?,?,?,?,?,?)",
          (today, r["shibor_overnight"], r["shibor_1w"], r["shibor_1m"], r["shibor_3m"], r["shibor_6m"], r["shibor_1y"]))
print(f"利率: OK")

conn.commit()
conn.close()
print("更新完成!")
