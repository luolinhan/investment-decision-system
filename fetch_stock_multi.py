"""
获取实时股票数据 - 多方法尝试
"""
import json
import time

# 方法1: 使用akshare
def fetch_with_akshare():
    print("\n=== 方法1: akshare ===")
    try:
        import akshare as ak
        # 获取A股实时数据
        df = ak.stock_zh_a_spot_em()
        print(f"获取到 {len(df)} 条A股数据")

        # 关注的股票代码
        watch_codes = {
            "603259": "药明康德",
            "600438": "通威股份",
            "601012": "隆基绿能",
            "002459": "晶澳科技",
            "300763": "锦浪科技",
            "688235": "百济神州",
            "600196": "复星医药",
            "601888": "中国中免",
        }

        results = []
        for code, name in watch_codes.items():
            row = df[df['代码'] == code]
            if len(row) > 0:
                r = row.iloc[0]
                results.append({
                    "code": f"sh{code}" if code.startswith('6') else f"sz{code}",
                    "name": name,
                    "market": "A",
                    "price": float(r['最新价']),
                    "change_pct": float(r['涨跌幅']),
                    "pe_ttm": float(r['市盈率-动态']) if r['市盈率-动态'] != '-' else None,
                    "pb": float(r['市净率']) if r['市净率'] != '-' else None,
                })
                print(f"{name}: 价格={r['最新价']} 涨跌={r['涨跌幅']}%")

        # 获取港股实时数据
        try:
            df_hk = ak.stock_hk_spot_em()
            print(f"获取到 {len(df_hk)} 条港股数据")

            watch_hk = {
                "02269": "药明生物",
                "06160": "百济神州",
                "01177": "中国生物制药",
                "01880": "中国中免",
                "00700": "腾讯控股",
                "03690": "美团-W",
                "01810": "小米集团-W",
                "01024": "快手-W",
                "09988": "阿里巴巴-W",
                "00883": "中国海洋石油",
            }

            for code, name in watch_hk.items():
                row = df_hk[df_hk['代码'] == code]
                if len(row) > 0:
                    r = row.iloc[0]
                    results.append({
                        "code": f"hk{code}",
                        "name": name,
                        "market": "HK",
                        "price": float(r['最新价']) if r['最新价'] != '-' else None,
                        "change_pct": float(r['涨跌幅']) if r['涨跌幅'] != '-' else None,
                    })
                    print(f"{name}: 价格={r['最新价']} 涨跌={r['涨跌幅']}%")
        except Exception as e:
            print(f"港股数据获取失败: {e}")

        return results
    except Exception as e:
        print(f"akshare方法失败: {e}")
        return None


# 方法2: 使用新浪API
def fetch_with_sina():
    print("\n=== 方法2: 新浪API ===")
    import urllib.request

    stocks = [
        ("sh603259", "药明康德"),
        ("sh600438", "通威股份"),
        ("sh601012", "隆基绿能"),
        ("sz002459", "晶澳科技"),
        ("sz300763", "锦浪科技"),
        ("sh688235", "百济神州"),
        ("sh600196", "复星医药"),
        ("sh601888", "中国中免"),
        ("hk02269", "药明生物"),
        ("hk06160", "百济神州"),
        ("hk01177", "中国生物制药"),
        ("hk01880", "中国中免"),
        ("hk00700", "腾讯控股"),
        ("hk03690", "美团-W"),
        ("hk01810", "小米集团-W"),
        ("hk01024", "快手-W"),
        ("hk09988", "阿里巴巴-W"),
        ("hk00883", "中国海洋石油"),
    ]

    results = []
    for code, name in stocks:
        try:
            # 新浪A股和港股格式不同
            if code.startswith('hk'):
                # 港股
                symbol = code[2:].zfill(5)
                url = f"http://hq.sinajs.cn/list=hk{symbol}"
            else:
                url = f"http://hq.sinajs.cn/list={code}"

            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0',
                'Referer': 'http://finance.sina.com.cn'
            })
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = resp.read().decode('gbk')
                # 解析数据
                if '=' in data and data.split('"')[1]:
                    parts = data.split('"')[1].split(',')
                    if len(parts) > 30:
                        price = float(parts[3]) if parts[3] else None
                        change_pct = float(parts[32]) if parts[32] else None
                        results.append({
                            "code": code,
                            "name": name,
                            "price": price,
                            "change_pct": change_pct,
                        })
                        print(f"{name}: 价格={price} 涨跌={change_pct}%")
        except Exception as e:
            print(f"{name}: 失败 - {e}")

    return results


# 方法3: 使用腾讯API
def fetch_with_tencent():
    print("\n=== 方法3: 腾讯API ===")
    import urllib.request

    stocks = [
        ("sh603259", "药明康德"),
        ("sh600438", "通威股份"),
        ("sh601012", "隆基绿能"),
        ("sz002459", "晶澳科技"),
        ("sz300763", "锦浪科技"),
        ("sh688235", "百济神州"),
        ("sh600196", "复星医药"),
        ("sh601888", "中国中免"),
        ("hk02269", "药明生物"),
        ("hk06160", "百济神州"),
        ("hk01177", "中国生物制药"),
        ("hk01880", "中国中免"),
        ("hk00700", "腾讯控股"),
        ("hk03690", "美团-W"),
        ("hk01810", "小米集团-W"),
        ("hk01024", "快手-W"),
        ("hk09988", "阿里巴巴-W"),
        ("hk00883", "中国海洋石油"),
    ]

    results = []
    # 批量请求
    codes = [s[0] for s in stocks]
    url = "https://qt.gtimg.cn/q=" + ",".join(codes)

    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0'
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode('gbk')

            # 解析
            lines = data.strip().split('\n')
            for line in lines:
                if '~' in line:
                    parts = line.split('~')
                    if len(parts) > 35:
                        code = parts[2]
                        name = parts[1]
                        price = float(parts[3]) if parts[3] else None
                        change_pct = float(parts[32]) if parts[32] else None
                        pe = float(parts[39]) if parts[39] and parts[39] != '-' else None
                        pb = float(parts[46]) if parts[46] and parts[46] != '-' else None

                        results.append({
                            "code": code,
                            "name": name,
                            "price": price,
                            "change_pct": change_pct,
                            "pe_ttm": pe,
                            "pb": pb,
                        })
                        print(f"{name}: 价格={price} 涨跌={change_pct}% PE={pe} PB={pb}")
    except Exception as e:
        print(f"腾讯API失败: {e}")

    return results


def main():
    print("=" * 60)
    print("股票数据采集 - 多方法尝试")
    print("=" * 60)

    # 尝试各种方法
    results = None

    # 方法1: akshare
    results = fetch_with_akshare()
    if results and len(results) >= 10:
        print(f"\nakshare成功获取 {len(results)} 条数据")
    else:
        # 方法2: 新浪
        results = fetch_with_sina()
        if results and len(results) >= 10:
            print(f"\n新浪API成功获取 {len(results)} 条数据")
        else:
            # 方法3: 腾讯
            results = fetch_with_tencent()
            if results and len(results) >= 10:
                print(f"\n腾讯API成功获取 {len(results)} 条数据")

    if results:
        print("\n" + "=" * 60)
        print(f"最终获取 {len(results)} 条数据")
        print("\n---JSON---")
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        print("\n所有方法都失败了！")


if __name__ == "__main__":
    main()