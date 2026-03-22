"""
使用OpenCLI获取金融数据
"""
import subprocess
import json
import sys

def run_opencli(command):
    """运行opencli命令"""
    try:
        result = subprocess.run(
            f"opencli {command}",
            shell=True,
            capture_output=True,
            text=True,
            timeout=60
        )
        return result.stdout
    except Exception as e:
        return f"Error: {e}"

def get_xueqiu_hot():
    """获取雪球热门股票"""
    print("=== 雪球热门股票 ===")
    output = run_opencli("xueqiu hot --limit 10 -f json")
    print(output)

def get_xueqiu_feed():
    """获取雪球关注动态"""
    print("=== 雪球关注动态 ===")
    output = run_opencli("xueqiu feed --limit 10 -f json")
    print(output)

def get_bloomberg_markets():
    """获取Bloomberg市场新闻"""
    print("=== Bloomberg市场新闻 ===")
    output = run_opencli("bloomberg markets -f json")
    print(output[:2000])

def get_bloomberg_economics():
    """获取Bloomberg经济新闻"""
    print("=== Bloomberg经济新闻 ===")
    output = run_opencli("bloomberg economics")
    print(output[:2000])

def get_barchart_quote(symbol):
    """获取Barchart股票报价"""
    print(f"=== Barchart {symbol} 报价 ===")
    output = run_opencli(f"barchart quote --symbol {symbol}")
    print(output)

def get_barchart_options(symbol):
    """获取Barchart期权数据"""
    print(f"=== Barchart {symbol} 期权 ===")
    output = run_opencli(f"barchart options --symbol {symbol}")
    print(output[:2000])


if __name__ == "__main__":
    print("OpenCLI 金融数据测试\n")

    # 测试公开API（不需要浏览器登录）
    get_bloomberg_markets()
    print("\n" + "="*50 + "\n")

    # 测试需要登录的API
    # get_xueqiu_hot()  # 需要Chrome登录雪球
    # get_barchart_quote("AAPL")  # 可能需要登录