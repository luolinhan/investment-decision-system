# 检查AKShare列名
import akshare as ak

df = ak.stock_research_report_em()
print("列名:", list(df.columns))
print("\n第一条数据:")
print(df.iloc[0])