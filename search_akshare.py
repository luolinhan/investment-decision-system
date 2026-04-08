"""搜索AkShare可用的全球市场数据接口"""
import akshare as ak

print("=" * 60)
print("AkShare 函数搜索 (全球/商品/债券相关)")
print("=" * 60)

# 搜索相关函数名
keywords = ['vix', 'bond', 'treasury', 'gold', 'oil', 'crude', 'commodity', 'us', 'usa', 'global', 'fx', 'future']

all_funcs = [name for name in dir(ak) if not name.startswith('_')]
print(f"\nAkShare总函数数: {len(all_funcs)}")

for kw in keywords:
    matched = [f for f in all_funcs if kw.lower() in f.lower()]
    if matched:
        print(f"\n'{kw}' 相关 ({len(matched)}个):")
        for f in matched[:15]:
            print(f"   - {f}")
        if len(matched) > 15:
            print(f"   ... 还有{len(matched)-15}个")

# 尝试实际调用一些可能有用的函数
print("\n" + "=" * 60)
print("实际调用测试")
print("=" * 60)

# 1. 尝试商品数据
print("\n1. 商品数据:")
try:
    funcs = ['commodity_name', 'commodity_spot', 'futures_commmodity_info']
    for f in funcs:
        if hasattr(ak, f):
            print(f"   尝试 {f}...")
            func = getattr(ak, f)
            try:
                result = func()
                if hasattr(result, 'head'):
                    print(f"   成功! 列: {result.columns.tolist()[:5]}")
                    print(f"   数据量: {len(result)}")
                else:
                    print(f"   结果: {result}")
            except Exception as e:
                print(f"   调用失败: {e}")
except Exception as e:
    print(f"   失败: {e}")

# 2. 尝试期货行情
print("\n2. 期货行情:")
try:
    if hasattr(ak, 'futures_zh_spot'):
        df = ak.futures_zh_spot()
        print(f"   futures_zh_spot成功! 列: {df.columns.tolist()}")
        # 查找黄金/原油
        gold = df[df['symbol'].str.contains('金', na=False)]
        oil = df[df['symbol'].str.contains('油', na=False)]
        print(f"   黄金相关: {len(gold)}条")
        print(f"   原油相关: {len(oil)}条")
except Exception as e:
    print(f"   失败: {e}")

# 3. 尝试外汇
print("\n3. 外汇数据:")
try:
    if hasattr(ak, 'fx_spot_quote'):
        df = ak.fx_spot_quote()
        print(f"   fx_spot_quote成功! 列: {df.columns.tolist()}")
except Exception as e:
    print(f"   失败: {e}")

# 4. 尝试美国数据
print("\n4. 美国市场:")
try:
    funcs = ['macro_usa_api', 'macro_usa_gdp', 'stock_us_index']
    for f in funcs:
        if hasattr(ak, f):
            print(f"   尝试 {f}...")
            func = getattr(ak, f)
            try:
                result = func()
                if hasattr(result, 'head'):
                    print(f"   成功! {f}")
                    print(result.head(2))
            except Exception as e:
                print(f"   调用失败: {e}")
except Exception as e:
    print(f"   失败: {e}")

print("\n完成!")