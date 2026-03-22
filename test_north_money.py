"""采集北向资金数据"""
import akshare as ak
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = "D:/research_report_system/data/investment.db"

def collect_north_money():
    """采集北向资金数据"""
    print("采集北向资金数据...")

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # 尝试使用东方财富的北向资金历史接口
        # 使用沪股通获取历史数据
        df = ak.stock_hsgt_hist_em(symbol='沪股通')

        if df is not None and len(df) > 0:
            print(f"获取到 {len(df)} 条数据")
            print(f"列名: {list(df.columns)}")

            # 查看最后几行数据
            print("\n最近数据:")
            print(df.tail(5))

            # 日期列
            df.iloc[:, 0] = df.iloc[:, 0].astype(str)

            cutoff = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
            df = df[df.iloc[:, 0] >= cutoff]

            added = 0
            for _, row in df.iterrows():
                try:
                    trade_date = str(row.iloc[0])[:10]
                    # 检查每一列的数据
                    print(f"{trade_date}: 列2={row.iloc[2]}, 列3={row.iloc[3]}, 列4={row.iloc[4]}, 列5={row.iloc[5]}, 列6={row.iloc[6]}")

                    # 尝试不同的列索引
                    for i in range(2, 8):
                        val = row.iloc[i]
                        if pd.notna(val) and val != 0:
                            print(f"  列{i}: {val}")
                except Exception as ex:
                    print(f"错误: {ex}")
                    continue

        conn.close()

    except Exception as e:
        print(f"失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    collect_north_money()