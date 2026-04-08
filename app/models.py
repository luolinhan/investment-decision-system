"""
研报下载系统 - 数据库模型
"""
from sqlalchemy import Column, Integer, String, Text, Date, DateTime, Boolean, Float, create_engine
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


class ForeignResearchSource(Base):
    """海外研报来源配置"""
    __tablename__ = "foreign_research_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String(100), nullable=False, unique=True, index=True)
    institution_name = Column(String(120), index=True)
    source_type = Column(String(30), default="public_web")  # public_web / authorized_portal / rss / manual
    base_url = Column(String(1000))
    list_url = Column(String(1000))
    login_required = Column(Boolean, default=False)
    enabled = Column(Boolean, default=True, index=True)
    crawl_frequency_minutes = Column(Integer, default=1440)
    config_json = Column(Text)
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class ForeignResearchDocument(Base):
    """海外研报原文与元数据"""
    __tablename__ = "foreign_research_documents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, index=True)
    external_id = Column(String(200), unique=True, index=True)
    source_name = Column(String(100), index=True)
    institution_name = Column(String(120), index=True)
    title_original = Column(String(1000), nullable=False)
    title_cn = Column(String(1000))
    title_en = Column(String(1000))
    author = Column(String(200))
    publish_date = Column(Date, index=True)
    language = Column(String(20), default="en")
    region = Column(String(30), default="global")
    market_scope = Column(String(50), default="macro")  # macro / industry / equity
    doc_type = Column(String(50), default="report")
    source_url = Column(String(1200))
    pdf_url = Column(String(1200))
    html_url = Column(String(1200))
    storage_path = Column(String(1000))
    file_hash = Column(String(128), unique=True, index=True)
    file_size = Column(Integer, default=0)
    page_count = Column(Integer, default=0)
    status = Column(String(30), default="new", index=True)
    retention_expires_at = Column(DateTime, index=True)
    raw_excerpt = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class ForeignResearchAnalysis(Base):
    """海外研报结构化分析"""
    __tablename__ = "foreign_research_analyses"

    id = Column(Integer, primary_key=True, autoincrement=True)
    document_id = Column(Integer, index=True, nullable=False)
    analysis_version = Column(String(50), default="v1")
    model_name = Column(String(100))
    prompt_version = Column(String(50), default="v1")
    translation_cn = Column(Text)
    summary_cn = Column(Text)
    summary_en = Column(Text)
    stance = Column(String(30), index=True)  # bullish / neutral / bearish
    confidence_score = Column(Float, default=0.0)
    key_points_json = Column(Text)
    drivers_json = Column(Text)
    risks_json = Column(Text)
    invalid_conditions_json = Column(Text)
    price_targets_json = Column(Text)
    rating_change = Column(String(100))
    macro_conclusion = Column(Text)
    industry_conclusion = Column(Text)
    equity_conclusion = Column(Text)
    topic_tags_json = Column(Text)
    entity_mentions_json = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class ForeignResearchTag(Base):
    """海外研报标签"""
    __tablename__ = "foreign_research_tags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_type = Column(String(50), index=True)  # macro / industry / company / stance / event
    tag_name = Column(String(120), nullable=False, unique=True, index=True)
    tag_name_cn = Column(String(120))
    parent_tag = Column(String(120))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class ForeignResearchDocumentTag(Base):
    """文档标签关联"""
    __tablename__ = "foreign_research_document_tags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    document_id = Column(Integer, index=True, nullable=False)
    tag_id = Column(Integer, index=True, nullable=False)
    confidence = Column(Float, default=0.0)
    source = Column(String(30), default="model")
    created_at = Column(DateTime, default=datetime.now)


class ForeignResearchBatch(Base):
    """采集/分析批次"""
    __tablename__ = "foreign_research_batches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_type = Column(String(30), index=True)  # crawl / sync / analyze
    source_scope = Column(String(100))
    date_from = Column(Date)
    date_to = Column(Date)
    document_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    fail_count = Column(Integer, default=0)
    status = Column(String(30), default="running", index=True)
    notes = Column(Text)
    started_at = Column(DateTime, default=datetime.now)
    finished_at = Column(DateTime)


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
