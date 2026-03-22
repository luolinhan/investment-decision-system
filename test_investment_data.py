# 测试AKShare获取投资决策所需数据 - 使用Clash代理
import akshare as ak
import pandas as pd
import os

# 设置代理
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'
os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'

pd.set_option('display.max_columns', 5)
pd.set_option('display.width', 100)

print("=== AKShare 投资决策数据测试 (使用代理) ===\n")

results = {}

# 1. 市场情绪
print("1. 市场情绪/活跃度:")
try:
    df = ak.stock_market_activity_legu()
    print(f"   [OK] 市场活跃度: {len(df)} 条")
    results['market_activity'] = True
except Exception as e:
    print(f"   [FAIL] {str(e)[:50]}")

# 2. Shibor利率
print("\n2. Shibor利率:")
try:
    df = ak.rate_interbank(market="Shibor", symbol="Shibor人民币报价")
    print(f"   [OK] Shibor: {len(df)} 条")
    print(f"   最新数据: {df.tail(1).to_string()[:100]}")
    results['shibor'] = True
except Exception as e:
    print(f"   [FAIL] {str(e)[:50]}")

# 3. 恒生指数
print("\n3. 恒生指数:")
try:
    df = ak.stock_hk_index_daily_em(symbol="HSI")
    print(f"   [OK] 恒生指数: {len(df)} 条历史数据")
    latest = df.tail(1)
    print(f"   最新收盘: {latest['close'].values[0]:.2f}")
    results['hk_index'] = True
except Exception as e:
    print(f"   [FAIL] {str(e)[:50]}")

# 4. 上证指数
print("\n4. 上证指数:")
try:
    df = ak.stock_zh_index_daily(symbol="sh000001")
    print(f"   [OK] 上证指数: {len(df)} 条历史数据")
    latest = df.tail(1)
    print(f"   最新收盘: {latest['close'].values[0]:.2f}")
    results['sh_index'] = True
except Exception as e:
    print(f"   [FAIL] {str(e)[:50]}")

# 5. VIX恐慌指数
print("\n5. VIX恐慌指数:")
try:
    df = ak.index_us_stock_sina(symbol="VIX")
    print(f"   [OK] VIX: {len(df)} 条")
    results['vix'] = True
except Exception as e:
    print(f"   [FAIL] {str(e)[:50]}")

# 6. 港股行情
print("\n6. 港股行情 (关注股票):")
try:
    df = ak.stock_hk_spot_em()
    print(f"   [OK] 港股行情: {len(df)} 条")

    # 查找关注的股票
    targets = ['阿里巴巴', '腾讯', '美团', '小米', '快手']
    for t in targets:
        found = df[df['名称'].str.contains(t, na=False)]
        if len(found) > 0:
            row = found.iloc[0]
            print(f"     {row['名称']}: 价格={row['最新价']}")
    results['hk_stocks'] = True
except Exception as e:
    print(f"   [FAIL] {str(e)[:80]}")

# 7. A股行情
print("\n7. A股行情 (关注股票):")
try:
    df = ak.stock_zh_a_spot_em()
    print(f"   [OK] A股行情: {len(df)} 条")

    # 查找光伏股票
    targets = ['晶澳科技', '通威股份', '隆基绿能', '锦浪科技']
    for t in targets:
        found = df[df['名称'].str.contains(t, na=False)]
        if len(found) > 0:
            row = found.iloc[0]
            pe = row.get('市盈率-动态', 'N/A')
            print(f"     {row['名称']}: 价格={row['最新价']}, 市盈率={pe}")
    results['a_stocks'] = True
except Exception as e:
    print(f"   [FAIL] {str(e)[:80]}")

# 8. 国债收益率
print("\n8. 国债收益率:")
try:
    df = ak.bond_zh_us_rate()
    print(f"   [OK] 国债收益率: {len(df)} 条")
    results['bond_yield'] = True
except Exception as e:
    print(f"   [FAIL] {str(e)[:50]}")

print("\n" + "="*50)
print("数据可用性汇总:")
for k, v in results.items():
    status = "OK" if v else "FAIL"
    print(f"  [{status}] {k}")