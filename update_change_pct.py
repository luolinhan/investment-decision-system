"""计算并更新指数涨跌幅"""
import sqlite3

DB_PATH = "data/investment.db"

def update_change_pct():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 获取所有指数代码
    c.execute("SELECT DISTINCT code FROM index_history")
    codes = [row[0] for row in c.fetchall()]

    total_updated = 0
    for code in codes:
        # 获取该指数所有数据按日期排序
        c.execute("""
            SELECT id, trade_date, close FROM index_history
            WHERE code = ?
            ORDER BY trade_date
        """, (code,))

        rows = c.fetchall()
        prev_close = None

        for row in rows:
            row_id, trade_date, close = row
            if prev_close is not None and prev_close > 0:
                change_pct = round((close - prev_close) / prev_close * 100, 2)
                c.execute("""
                    UPDATE index_history SET change_pct = ? WHERE id = ?
                """, (change_pct, row_id))
                total_updated += 1
            prev_close = close

    conn.commit()
    conn.close()
    print(f"更新了 {total_updated} 条涨跌幅数据")

    # 验证
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT code, name, trade_date, close, change_pct
        FROM index_history
        WHERE trade_date >= '2026-03-19'
        ORDER BY code, trade_date DESC
    """)
    print("\n验证结果:")
    for row in c.fetchall()[:15]:
        print(f"  {row[1]} {row[2]}: {row[3]:.2f} ({row[4]}%)")
    conn.close()

if __name__ == "__main__":
    update_change_pct()