# 测试券商研报接口
import akshare as ak
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

print("测试 stock_research_report_em 券商研报接口:\n")

# 获取研报数据
try:
    df = ak.stock_research_report_em()
    print(f"获取到 {len(df)} 条研报")
    print(f"\n列名: {list(df.columns)}")
    print(f"\n前5条数据:")
    print(df.head())

    # 筛选港股相关
    print("\n\n筛选包含'阿里'的研报:")
    ali = df[df['标题'].str.contains('阿里', na=False)]
    print(f"找到 {len(ali)} 条")
    if len(ali) > 0:
        print(ali[['标题', '机构', '发布时间']].head())

except Exception as e:
    print(f"错误: {e}")
    import traceback
    traceback.print_exc()