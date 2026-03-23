"""
关注标的与基准定义
"""
from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class Instrument:
    code: str
    finance_code: str
    report_code: str
    name: str
    market: str
    category: str
    yahoo_symbol: str
    aliases: List[str] = field(default_factory=list)


WATCHLIST = [
    Instrument("hk09988", "hk09988", "09988.HK", "阿里巴巴", "HK", "科技", "9988.HK", ["阿里巴巴-W"]),
    Instrument("hk00700", "hk00700", "00700.HK", "腾讯控股", "HK", "科技", "0700.HK", ["腾讯"]),
    Instrument("hk03690", "hk03690", "03690.HK", "美团", "HK", "科技", "3690.HK", ["美团-W"]),
    Instrument("hk01810", "hk01810", "01810.HK", "小米集团", "HK", "科技", "1810.HK", ["小米", "小米集团-W"]),
    Instrument("hk01024", "hk01024", "01024.HK", "快手", "HK", "科技", "1024.HK", ["快手-W"]),
    Instrument("hk06160", "hk06160", "06160.HK", "百济神州", "HK", "医药", "6160.HK", []),
    Instrument("hk02269", "hk02269", "02269.HK", "药明生物", "HK", "CXO", "2269.HK", []),
    Instrument("hk00883", "hk00883", "00883.HK", "中国海洋石油", "HK", "能源", "0883.HK", ["中海油"]),
    Instrument("sh603259", "sh603259", "603259.SH", "药明康德", "A", "CXO", "603259.SS", []),
    Instrument("sh600438", "sh600438", "600438.SH", "通威股份", "A", "光伏", "600438.SS", []),
    Instrument("sh601012", "sh601012", "601012.SH", "隆基绿能", "A", "光伏", "601012.SS", []),
    Instrument("sz002459", "sz002459", "002459.SZ", "晶澳科技", "A", "光伏", "002459.SZ", []),
    Instrument("sz300763", "sz300763", "300763.SZ", "锦浪科技", "A", "光伏", "300763.SZ", []),
    Instrument("sh688235", "sh688235", "688235.SH", "百济神州A", "A", "医药", "688235.SS", ["百济神州"]),
    Instrument("sh600196", "sh600196", "600196.SH", "复星医药", "A", "医药", "600196.SS", []),
    Instrument("sh601888", "sh601888", "601888.SH", "中国中免", "A", "消费", "601888.SS", []),
]


BENCHMARKS = [
    {"code": "hsi", "name": "恒生指数", "yahoo_symbol": "^HSI"},
    {"code": "fxi", "name": "中国大盘ETF代理", "yahoo_symbol": "FXI"},
    {"code": "yinn", "name": "YINN", "yahoo_symbol": "YINN"},
    {"code": "yang", "name": "YANG", "yahoo_symbol": "YANG"},
    {"code": "vix", "name": "VIX", "yahoo_symbol": "^VIX"},
]

