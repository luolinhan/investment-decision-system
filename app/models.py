"""
研报下载系统 - 数据库模型
"""
from sqlalchemy import Column, Integer, String, Text, Date, DateTime, Boolean, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()


class Stock(Base):
    """关注的股票"""
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(20), unique=True, nullable=False, index=True)  # 如 09988.HK
    name = Column(String(50), nullable=False)
    market = Column(String(10))  # HK/A/US
    category = Column(String(50))  # 科技/医药/光伏
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class Report(Base):
    """研报"""
    __tablename__ = "reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(500), nullable=False)
    stock_code = Column(String(20), index=True)
    stock_name = Column(String(50))
    institution = Column(String(100), index=True)  # 研报机构
    author = Column(String(100))  # 分析师
    rating = Column(String(50))  # 评级
    publish_date = Column(Date, index=True)
    pdf_url = Column(String(1000))
    local_pdf_path = Column(String(500))  # 本地PDF路径
    summary = Column(Text)  # AI摘要
    raw_content = Column(Text)  # 原文内容
    source = Column(String(50))  # 来源：eastmoney/hibor/xueqiu
    external_id = Column(String(100), unique=True, index=True)  # 外部ID，用于去重
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class CollectionTask(Base):
    """采集任务记录"""
    __tablename__ = "collection_tasks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20))
    source = Column(String(50))
    status = Column(String(20))  # running/completed/failed
    report_count = Column(Integer, default=0)
    error_message = Column(Text)
    started_at = Column(DateTime, default=datetime.now)
    completed_at = Column(DateTime)


# 初始股票数据
DEFAULT_STOCKS = [
    # 科技 / 半导体
    {"code": "sh688981", "name": "中芯国际", "market": "A", "category": "半导体"},
    {"code": "sz002475", "name": "立讯精密", "market": "A", "category": "电子"},
    {"code": "sh603501", "name": "韦尔股份", "market": "A", "category": "半导体"},
    {"code": "sh688111", "name": "金山办公", "market": "A", "category": "软件"},
    {"code": "sz002230", "name": "科大讯飞", "market": "A", "category": "AI"},
    {"code": "sh603986", "name": "兆易创新", "market": "A", "category": "半导体"},

    # 新能源 / 光伏 / 锂电
    {"code": "sz300750", "name": "宁德时代", "market": "A", "category": "锂电"},
    {"code": "sz002459", "name": "晶澳科技", "market": "A", "category": "光伏"},
    {"code": "sh600438", "name": "通威股份", "market": "A", "category": "光伏"},
    {"code": "sh601012", "name": "隆基绿能", "market": "A", "category": "光伏"},
    {"code": "sz300763", "name": "锦浪科技", "market": "A", "category": "光伏"},
    {"code": "sz300014", "name": "亿纬锂能", "market": "A", "category": "锂电"},
    {"code": "sz002460", "name": "赣锋锂业", "market": "A", "category": "锂电"},

    # 消费 / 白酒
    {"code": "sh600519", "name": "贵州茅台", "market": "A", "category": "白酒"},
    {"code": "sz000858", "name": "五粮液", "market": "A", "category": "白酒"},
    {"code": "sh603288", "name": "海天味业", "market": "A", "category": "食品"},
    {"code": "sh601888", "name": "中国中免", "market": "A", "category": "消费"},

    # 医药 / CXO
    {"code": "sh688235", "name": "百济神州", "market": "A", "category": "创新药"},
    {"code": "sh603259", "name": "药明康德", "market": "A", "category": "CXO"},
    {"code": "sz300760", "name": "迈瑞医疗", "market": "A", "category": "医疗器械"},
    {"code": "sh600276", "name": "恒瑞医药", "market": "A", "category": "创新药"},
    {"code": "sh600196", "name": "复星医药", "market": "A", "category": "医药"},

    # 金融
    {"code": "sh600036", "name": "招商银行", "market": "A", "category": "银行"},
    {"code": "sh601318", "name": "中国平安", "market": "A", "category": "保险"},
    {"code": "sh600030", "name": "中信证券", "market": "A", "category": "券商"},

    # 制造 / 工业
    {"code": "sz000333", "name": "美的集团", "market": "A", "category": "家电"},
    {"code": "sh601899", "name": "紫金矿业", "market": "A", "category": "有色"},
    {"code": "sz002352", "name": "顺丰控股", "market": "A", "category": "物流"},

    # 港股恒生科技
    {"code": "09988.HK", "name": "阿里巴巴-W", "market": "HK", "category": "科技"},
    {"code": "00700.HK", "name": "腾讯控股", "market": "HK", "category": "科技"},
    {"code": "03690.HK", "name": "美团-W", "market": "HK", "category": "科技"},
    {"code": "01810.HK", "name": "小米集团-W", "market": "HK", "category": "科技"},
    {"code": "01024.HK", "name": "快手-W", "market": "HK", "category": "科技"},
    {"code": "09618.HK", "name": "京东集团-SW", "market": "HK", "category": "科技"},
    {"code": "09888.HK", "name": "百度集团-SW", "market": "HK", "category": "科技"},

    # 港股医药
    {"code": "06160.HK", "name": "百济神州", "market": "HK", "category": "创新药"},
    {"code": "01801.HK", "name": "信达生物", "market": "HK", "category": "创新药"},
    {"code": "02269.HK", "name": "药明生物", "market": "HK", "category": "CXO"},
    {"code": "01177.HK", "name": "中国生物制药", "market": "HK", "category": "医药"},
    {"code": "02196.HK", "name": "复星医药", "market": "HK", "category": "医药"},

    # 港股能源 / 高股息
    {"code": "00883.HK", "name": "中国海洋石油", "market": "HK", "category": "能源"},
    {"code": "00941.HK", "name": "中国移动", "market": "HK", "category": "通信"},
    {"code": "01299.HK", "name": "友邦保险", "market": "HK", "category": "保险"},

    # 港股汽车
    {"code": "01211.HK", "name": "比亚迪股份", "market": "HK", "category": "汽车"},
    {"code": "02015.HK", "name": "理想汽车-W", "market": "HK", "category": "汽车"},
    {"code": "09868.HK", "name": "小鹏汽车-W", "market": "HK", "category": "汽车"},
]
