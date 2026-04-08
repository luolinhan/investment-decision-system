"""检查AkShare可用的全球市场数据接口"""
import akshare as ak
import pandas as pd

print("=" * 60)
print("AkShare 全球市场数据接口检查")
print("=" * 60)

# 1. VIX恐慌指数
print("\n1. VIX恐慌指数接口:")
try:
    # 尝试不同的接口名
    funcs = [
        ("index_vix", "VIX历史数据"),
        ("index_us_stock_sina", "美股指数(新浪)"),
        ("index_global_sina", "全球指数(新浪)"),
    ]
    for func_name, desc in funcs:
        try:
            func = getattr(ak, func_name)
            print(f"   {func_name} ({desc}): 存在")
            if func_name == "index_global_sina":
                df = func()
                print(f"      包含指数: {df['name'].unique()[:10].tolist()}")
        except AttributeError:
            print(f"   {func_name}: 不存在")
except Exception as e:
    print(f"   检查失败: {e}")

# 2. 美债收益率
print("\n2. 美债收益率接口:")
try:
    funcs = [
        ("bond_us_treasury_yield", "美债收益率曲线"),
        ("bond_us_treasury_rate", "美债利率"),
        ("macro_board_usa", "美国宏观经济"),
        ("macro_usa_interest_rate", "美国利率"),
    ]
    for func_name, desc in funcs:
        try:
            func = getattr(ak, func_name)
            print(f"   {func_name} ({desc}): 存在")
            if func_name == "bond_us_treasury_yield":
                df = func()
                print(f"      列: {df.columns.tolist()}")
        except AttributeError:
            print(f"   {func_name}: 不存在")
except Exception as e:
    print(f"   检查失败: {e}")

# 3. 黄金价格
print("\n3. 黄金价格接口:")
try:
    funcs = [
        ("fx_spot_gold", "现货黄金"),
        ("gold_spot_hist", "黄金历史"),
        ("fx_hist_gold_sina", "黄金历史(新浪)"),
        ("quote_gold_hist", "黄金历史行情"),
    ]
    for func_name, desc in funcs:
        try:
            func = getattr(ak, func_name)
            print(f"   {func_name} ({desc}): 存在")
        except AttributeError:
            print(f"   {func_name}: 不存在")
except Exception as e:
    print(f"   检查失败: {e}")

# 4. 原油价格
print("\n4. 原油价格接口:")
try:
    funcs = [
        ("energy_oil_price", "原油价格"),
        ("oil_hist", "原油历史"),
        ("crude_oil_hist", "原油期货历史"),
        ("future_oil_hist", "原油期货"),
    ]
    for func_name, desc in funcs:
        try:
            func = getattr(ak, func_name)
            print(f"   {func_name} ({desc}): 存在")
        except AttributeError:
            print(f"   {func_name}: 不存在")
except Exception as e:
    print(f"   检查失败: {e}")

# 5. 实际测试一些接口
print("\n" + "=" * 60)
print("实际数据获取测试")
print("=" * 60)

# 测试全球指数
print("\n测试 index_global_sina:")
try:
    df = ak.index_global_sina()
    vix_data = df[df['name'].str.contains('VIX', case=False, na=False)]
    print(f"   VIX数据: {vix_data.head() if len(vix_data) > 0 else '无VIX'}")
except Exception as e:
    print(f"   失败: {e}")

# 测试美债
print("\n测试 bond_us_treasury_yield:")
try:
    df = ak.bond_us_treasury_yield()
    print(f"   数据: {df.head(3)}")
except Exception as e:
    print(f"   失败: {e}")

# 测试商品
print("\n测试 spot_商品:")
try:
    df = ak.fx_spot_quote()  # 外汇现货
    gold = df[df['name'].str.contains('金', na=False)]
    print(f"   黄金相关: {gold.head(3) if len(gold) > 0 else '无'}")
except Exception as e:
    print(f"   失败: {e}")

print("\n完成!")