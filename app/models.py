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


class Setting(Base):
    """系统设置"""
    __tablename__ = "settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(100), unique=True, nullable=False)
    value = Column(Text)
    description = Column(String(500))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# 初始股票数据
DEFAULT_STOCKS = [
    # 港股恒生科技
    {"code": "09988.HK", "name": "阿里巴巴-W", "market": "HK", "category": "科技"},
    {"code": "00700.HK", "name": "腾讯控股", "market": "HK", "category": "科技"},
    {"code": "03690.HK", "name": "美团-W", "market": "HK", "category": "科技"},
    {"code": "01024.HK", "name": "快手-W", "market": "HK", "category": "科技"},
    {"code": "01810.HK", "name": "小米集团-W", "market": "HK", "category": "科技"},

    # 港股其他
    {"code": "01880.HK", "name": "中国中免", "market": "HK", "category": "消费"},
    {"code": "00670.HK", "name": "中国东方航空", "market": "HK", "category": "航空"},
    {"code": "00883.HK", "name": "中国海洋石油", "market": "HK", "category": "能源"},

    # 港股医药
    {"code": "06160.HK", "name": "百济神州", "market": "HK", "category": "创新药"},
    {"code": "09995.HK", "name": "荣昌生物", "market": "HK", "category": "创新药"},
    {"code": "01811.HK", "name": "信达生物", "market": "HK", "category": "创新药"},
    {"code": "01177.HK", "name": "中国生物制药", "market": "HK", "category": "创新药"},
    {"code": "02196.HK", "name": "复星医药", "market": "HK", "category": "医药"},
    {"code": "02359.HK", "name": "药明康德", "market": "HK", "category": "CXO"},
    {"code": "02269.HK", "name": "药明生物", "market": "HK", "category": "CXO"},
    {"code": "06127.HK", "name": "昭衍新药", "market": "HK", "category": "CXO"},
    {"code": "03320.HK", "name": "华润医药", "market": "HK", "category": "医药"},

    # A股光伏
    {"code": "002459.SZ", "name": "晶澳科技", "market": "A", "category": "光伏"},
    {"code": "600438.SH", "name": "通威股份", "market": "A", "category": "光伏"},
    {"code": "601012.SH", "name": "隆基绿能", "market": "A", "category": "光伏"},
    {"code": "300763.SZ", "name": "锦浪科技", "market": "A", "category": "光伏"},
]