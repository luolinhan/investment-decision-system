"""测试AkShare实际可用的全球市场接口"""
import akshare as ak
import pandas as pd

print("=" * 60)
print("实际数据获取测试")
print("=" * 60)

# 1. 全球指数 - 可能包含VIX
print("\n1. 全球指数 (index_global_spot_em):")
try:
    df = ak.index_global_spot_em()
    print(f"   列: {df.columns.tolist()}")
    print(f"   名称样本: {df['名称'].unique()[:20].tolist() if '名称' in df.columns else df.head(3)}")
    # 查找VIX
    if '名称' in df.columns:
        vix = df[df['名称'].str.contains('VIX|波动|恐慌', case=False, na=False)]
        print(f"   VIX相关: {len(vix)}条")
        if len(vix) > 0:
            print(vix)
except Exception as e:
    print(f"   失败: {e}")

# 2. 全球指数历史
print("\n2. 全球指数历史 (index_global_hist_em):")
try:
    # 尝试获取VIX历史，symbol可能是VIX
    df = ak.index_global_hist_em(symbol="VIX")
    print(f"   成功! 列: {df.columns.tolist()}")
    print(df.tail(5))
except Exception as e:
    print(f"   VIX失败: {e}")
    # 尝试其他symbol
    try:
        df = ak.index_global_hist_em(symbol="道琼斯")
        print(f"   道琼斯成功: {df.columns.tolist()}")
    except Exception as e2:
        print(f"   道琼斯也失败: {e2}")

# 3. 上海黄金交易所
print("\n3. 黄金基准价 (spot_golden_benchmark_sge):")
try:
    df = ak.spot_golden_benchmark_sge()
    print(f"   成功! 列: {df.columns.tolist()}")
    print(df.tail(5))
except Exception as e:
    print(f"   失败: {e}")

# 4. 黄金历史
print("\n4. 黄金历史 (macro_china_fx_gold):")
try:
    df = ak.macro_china_fx_gold()
    print(f"   成功! 列: {df.columns.tolist()}")
    print(df.head(5))
except Exception as e:
    print(f"   失败: {e}")

# 5. 原油历史
print("\n5. 原油历史 (energy_oil_hist):")
try:
    df = ak.energy_oil_hist()
    print(f"   成功! 列: {df.columns.tolist()}")
    print(df.tail(5))
except Exception as e:
    print(f"   失败: {e}")

# 6. 原油详情
print("\n6. 原油详情 (energy_oil_detail):")
try:
    df = ak.energy_oil_detail()
    print(f"   成功! 列: {df.columns.tolist()}")
    print(df.head(5))
except Exception as e:
    print(f"   失败: {e}")

# 7. 美国利率
print("\n7. 美国利率 (macro_bank_usa_interest_rate):")
try:
    df = ak.macro_bank_usa_interest_rate()
    print(f"   成功! 列: {df.columns.tolist()}")
    print(df.tail(10))
except Exception as e:
    print(f"   失败: {e}")

# 8. 美国国债利率
print("\n8. 美国国债 (bond_gb_us_sina):")
try:
    df = ak.bond_gb_us_sina()
    print(f"   成功! 列: {df.columns.tolist()}")
    print(df.head(5))
except Exception as e:
    print(f"   失败: {e}")

# 9. 中美汇率
print("\n9. 中美汇率 (bond_zh_us_rate):")
try:
    df = ak.bond_zh_us_rate()
    print(f"   成功! 列: {df.columns.tolist()}")
    print(df.tail(5))
except Exception as e:
    print(f"   失败: {e}")

# 10. 全球商品期货
print("\n10. 全球商品期货 (futures_global_spot_em):")
try:
    df = ak.futures_global_spot_em()
    print(f"   成功! 列: {df.columns.tolist()}")
    # 查找黄金和原油
    if '名称' in df.columns:
        gold = df[df['名称'].str.contains('金', na=False)]
        oil = df[df['名称'].str.contains('油|原油|WTI|布油', na=False)]
        print(f"   黄金相关: {len(gold)}条")
        print(f"   原油相关: {len(oil)}条")
        if len(gold) > 0:
            print("   黄金样本:", gold.head(2).to_dict('records'))
        if len(oil) > 0:
            print("   原油样本:", oil.head(2).to_dict('records'))
except Exception as e:
    print(f"   失败: {e}")

# 11. 外汇现货报价
print("\n11. 外汇现货 (fx_spot_quote):")
try:
    df = ak.fx_spot_quote()
    print(f"   成功! 列: {df.columns.tolist()}")
    # 查找美元相关
    if '货币名称' in df.columns:
        usd = df[df['货币名称'].str.contains('美元|USD', na=False)]
        print(f"   美元相关: {len(usd)}条")
        print(usd.head(5))
except Exception as e:
    print(f"   失败: {e}")

print("\n完成!")