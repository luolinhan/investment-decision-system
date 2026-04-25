"""
短线服务 — 美股->A股/港股跨市场信号映射与候选生成。

使用 SQLite (data/investment.db) 存储三张表：
  - cross_market_mapping_master   US->CN 映射主表
  - cross_market_signal_events    美股结构化事件
  - cross_market_signal_candidates 生成的候选标的
"""
import sqlite3
import os
from datetime import datetime, date, timedelta
from typing import Optional
from app.db import get_sqlite_connection

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "investment.db")

# ---------------------------------------------------------------------------
# Seed data: 30+ high-quality US->CN mappings
# ---------------------------------------------------------------------------
SEED_MAPPINGS = [
    # --- AI / LLM ---
    ("MSFT", "sh688111", "AI应用", "AI", 0.92, "微软Copilot生态映射金山办公/AI软件"),
    ("GOOG", "sz002230", "AI应用", "AI", 0.90, "Google AI/搜索映射科大讯飞"),
    ("GOOG", "09988.HK", "AI应用", "AI", 0.88, "Google AI映射阿里云/通义"),
    ("META", "09988.HK", "AI社交", "AI", 0.82, "Meta AI映射阿里AI生态"),
    ("AAPL", "00700.HK", "AI终端", "AI", 0.78, "Apple Intelligence映射腾讯AI应用"),
    ("PLTR", "sz002230", "AI分析", "AI", 0.85, "Palantir AI分析映射科大讯飞"),
    # --- 半导体 ---
    ("NVDA", "sh688981", "GPU算力", "半导体", 0.95, "英伟达GPU映射中芯国际"),
    ("NVDA", "sh603501", "GPU算力", "半导体", 0.90, "英伟达GPU映射韦尔股份"),
    ("NVDA", "sh603986", "GPU算力", "半导体", 0.88, "英伟达GPU映射兆易创新"),
    ("AMD", "sh688981", "CPU/GPU", "半导体", 0.85, "AMD映射中芯国际"),
    ("AVGO", "sh603986", "网络芯片", "半导体", 0.80, "博通映射兆易创新"),
    ("TSM", "sh688981", "晶圆代工", "半导体", 0.93, "台积电映射中芯国际"),
    ("INTC", "sh688981", "IDM", "半导体", 0.75, "英特尔映射中芯国际"),
    ("QCOM", "sz002475", "移动芯片", "半导体", 0.82, "高通映射立讯精密/果链"),
    ("ARM", "sh603986", "IP授权", "半导体", 0.80, "ARM架构映射兆易创新"),
    # --- 机器人 ---
    ("TSLA", "sz002475", "人形机器人", "机器人", 0.85, "Tesla Optimus映射立讯精密供应链"),
    ("TSLA", "09988.HK", "自动驾驶", "机器人", 0.78, "Tesla FSD映射阿里自动驾驶"),
    # --- 创新药 ---
    ("LLY", "sh688235", "减肥药", "创新药", 0.88, "礼来GLP-1映射百济神州"),
    ("LLY", "sh600276", "减肥药", "创新药", 0.82, "礼来GLP-1映射恒瑞医药"),
    ("NVO", "sh688235", "减肥药", "创新药", 0.85, "诺和诺德GLP-1映射百济神州"),
    ("NVO", "sh600276", "减肥药", "创新药", 0.80, "诺和诺德映射恒瑞医药"),
    ("MRNA", "sh688235", "mRNA疫苗", "创新药", 0.78, "Moderna映射百济神州"),
    ("PFE", "sh600276", "创新药", "创新药", 0.72, "辉瑞映射恒瑞医药"),
    # --- 光伏 ---
    ("ENPH", "sz002459", "微型逆变器", "光伏", 0.85, "Enphase映射晶澳科技"),
    ("ENPH", "sh601012", "微型逆变器", "光伏", 0.82, "Enphase映射隆基绿能"),
    ("SEDG", "sz300763", "逆变器", "光伏", 0.80, "SolarEdge映射锦浪科技"),
    ("FSLR", "sh600438", "薄膜电池", "光伏", 0.78, "First Solar映射通威股份"),
    ("CSIQ", "sz002459", "组件", "光伏", 0.82, "阿特斯映射晶澳科技"),
    # --- 核电 ---
    ("CCJ", "sh601899", "铀矿", "核电", 0.75, "Cameco铀价映射紫金矿业资源品"),
    ("VST", "sh601899", "核电运营", "核电", 0.70, "Vistra核电映射紫金矿业"),
    ("OKLO", "sh601899", "小型堆", "核电", 0.68, "Oklo小型堆映射紫金矿业"),
    # --- 养猪 ---
    ("TSN", "sz002714", "肉类加工", "养猪", 0.78, "泰森食品映射牧原股份"),
    ("HRL", "sz002714", "肉制品", "养猪", 0.72, "Hormel映射牧原股份"),
    # --- 红利/高股息 ---
    ("XOM", "sh601899", "能源红利", "红利", 0.75, "埃克森美孚高股息映射紫金矿业"),
    ("CVX", "sh601899", "能源红利", "红利", 0.72, "雪佛龙高股息映射紫金矿业"),
    ("JPM", "sh600036", "金融红利", "红利", 0.80, "摩根大通映射招商银行"),
    ("KO", "sh600519", "消费红利", "红利", 0.78, "可口可乐高股息映射贵州茅台"),
    ("PG", "sh603288", "消费红利", "红利", 0.75, "宝洁高股息映射海天味业"),
    ("O", "sh601899", "REIT红利", "红利", 0.70, "Realty Income映射资源品红利"),
]

# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------
DDL_MAPPING_MASTER = """\
CREATE TABLE IF NOT EXISTS cross_market_mapping_master (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    us_ticker   TEXT NOT NULL,
    cn_code     TEXT NOT NULL,
    cn_name     TEXT,
    signal_type TEXT,
    sector      TEXT,
    confidence  REAL DEFAULT 0.5,
    rationale   TEXT,
    enabled     INTEGER DEFAULT 1,
    created_at  TEXT DEFAULT (datetime('now')),
    updated_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(us_ticker, cn_code)
);
"""

DDL_SIGNAL_EVENTS = """\
CREATE TABLE IF NOT EXISTS cross_market_signal_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    us_ticker       TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    event_date      TEXT NOT NULL,
    severity        REAL DEFAULT 0.5,
    detail_json     TEXT,
    source          TEXT DEFAULT 'yfinance',
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(us_ticker, event_type, event_date)
);
"""

DDL_SIGNAL_CANDIDATES = """\
CREATE TABLE IF NOT EXISTS cross_market_signal_candidates (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    us_ticker           TEXT NOT NULL,
    cn_code             TEXT NOT NULL,
    cn_name             TEXT,
    sector              TEXT,
    event_type          TEXT NOT NULL,
    event_date          TEXT NOT NULL,
    execution_priority  REAL DEFAULT 0.0,
    rationale           TEXT,
    created_at          TEXT DEFAULT (datetime('now')),
    UNIQUE(us_ticker, cn_code, event_type, event_date)
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_mapping_us ON cross_market_mapping_master(us_ticker);",
    "CREATE INDEX IF NOT EXISTS idx_mapping_cn ON cross_market_mapping_master(cn_code);",
    "CREATE INDEX IF NOT EXISTS idx_mapping_sector ON cross_market_mapping_master(sector);",
    "CREATE INDEX IF NOT EXISTS idx_events_ticker ON cross_market_signal_events(us_ticker);",
    "CREATE INDEX IF NOT EXISTS idx_events_type ON cross_market_signal_events(event_type);",
    "CREATE INDEX IF NOT EXISTS idx_candidates_priority ON cross_market_signal_candidates(execution_priority DESC);",
    "CREATE INDEX IF NOT EXISTS idx_candidates_cn ON cross_market_signal_candidates(cn_code);",
]


def ensure_tables(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Ensure the three cross-market tables exist. Returns the connection."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = get_sqlite_connection(db_path)
    conn.execute(DDL_MAPPING_MASTER)
    conn.execute(DDL_SIGNAL_EVENTS)
    conn.execute(DDL_SIGNAL_CANDIDATES)
    for ddl in DDL_INDEXES:
        conn.execute(ddl)
    conn.commit()
    return conn


def seed_mappings(conn: sqlite3.Connection, mappings: list[tuple] = SEED_MAPPINGS) -> int:
    """Insert seed mappings, skipping duplicates. Returns number inserted."""
    inserted = 0
    for us_ticker, cn_code, signal_type, sector, confidence, rationale in mappings:
        conn.execute(
            """INSERT OR IGNORE INTO cross_market_mapping_master
               (us_ticker, cn_code, signal_type, sector, confidence, rationale)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (us_ticker, cn_code, signal_type, sector, confidence, rationale),
        )
        inserted += 1
    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Service class
# ---------------------------------------------------------------------------
class ShortlineService:
    """跨市场短线信号服务。"""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = ensure_tables(db_path)

    # -- overview --
    def get_overview(self) -> dict:
        """Return summary statistics."""
        c = self.conn.cursor()
        mapping_count = c.execute("SELECT COUNT(*) FROM cross_market_mapping_master WHERE enabled=1").fetchone()[0]
        event_count = c.execute("SELECT COUNT(*) FROM cross_market_signal_events").fetchone()[0]
        candidate_count = c.execute("SELECT COUNT(*) FROM cross_market_signal_candidates").fetchone()[0]
        sectors = [
            r[0] for r in c.execute(
                "SELECT DISTINCT sector FROM cross_market_mapping_master WHERE enabled=1 ORDER BY sector"
            ).fetchall()
        ]
        us_tickers = [
            r[0] for r in c.execute(
                "SELECT DISTINCT us_ticker FROM cross_market_mapping_master WHERE enabled=1 ORDER BY us_ticker"
            ).fetchall()
        ]
        return {
            "mapping_count": mapping_count,
            "event_count": event_count,
            "candidate_count": candidate_count,
            "sectors": sectors,
            "us_tickers": us_tickers,
        }

    # -- candidates --
    def list_candidates(
        self,
        sector: Optional[str] = None,
        min_priority: float = 0.0,
        limit: int = 50,
    ) -> list[dict]:
        """List execution candidates, optionally filtered."""
        c = self.conn.cursor()
        sql = "SELECT us_ticker, cn_code, cn_name, sector, event_type, event_date, execution_priority, rationale FROM cross_market_signal_candidates WHERE 1=1"
        params: list = []
        if sector:
            sql += " AND sector = ?"
            params.append(sector)
        if min_priority > 0:
            sql += " AND execution_priority >= ?"
            params.append(min_priority)
        sql += " ORDER BY execution_priority DESC LIMIT ?"
        params.append(limit)
        rows = c.execute(sql, params).fetchall()
        return [
            {
                "us_ticker": r[0],
                "cn_code": r[1],
                "cn_name": r[2],
                "sector": r[3],
                "event_type": r[4],
                "event_date": r[5],
                "execution_priority": r[6],
                "rationale": r[7],
            }
            for r in rows
        ]

    # -- events --
    def list_events(
        self,
        event_type: Optional[str] = None,
        us_ticker: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """List signal events."""
        c = self.conn.cursor()
        sql = "SELECT us_ticker, event_type, event_date, severity, detail_json FROM cross_market_signal_events WHERE 1=1"
        params: list = []
        if event_type:
            sql += " AND event_type = ?"
            params.append(event_type)
        if us_ticker:
            sql += " AND us_ticker = ?"
            params.append(us_ticker)
        sql += " ORDER BY event_date DESC LIMIT ?"
        params.append(limit)
        rows = c.execute(sql, params).fetchall()
        return [
            {
                "us_ticker": r[0],
                "event_type": r[1],
                "event_date": r[2],
                "severity": r[3],
                "detail_json": r[4],
            }
            for r in rows
        ]

    # -- playbooks --
    def list_playbooks(self) -> list[dict]:
        """Return signal playbooks — predefined signal_type->event_type mappings with action templates."""
        return [
            {
                "signal_type": "GPU算力",
                "event_types": ["price_breakout", "etf_breakout"],
                "action": "追踪半导体/算力板块跟随标的",
                "hold_days": 5,
            },
            {
                "signal_type": "AI应用",
                "event_types": ["price_breakout", "sector_rotation"],
                "action": "关注AI应用/软件板块情绪扩散",
                "hold_days": 3,
            },
            {
                "signal_type": "减肥药",
                "event_types": ["earnings_spillover"],
                "action": "跟踪创新药/减肥药管线进展",
                "hold_days": 10,
            },
            {
                "signal_type": "微型逆变器",
                "event_types": ["price_breakout", "etf_breakout"],
                "action": "跟踪光伏出口链订单预期",
                "hold_days": 5,
            },
            {
                "signal_type": "人形机器人",
                "event_types": ["price_breakout", "sector_rotation"],
                "action": "跟踪机器人产业链情绪",
                "hold_days": 7,
            },
            {
                "signal_type": "能源红利",
                "event_types": ["sector_rotation"],
                "action": "高股息板块防御配置",
                "hold_days": 20,
            },
        ]

    # -- sync --
    def sync_us_market_events(
        self,
        tickers: Optional[list[str]] = None,
        days: int = 5,
    ) -> dict:
        """
        Pull US market data via yfinance, detect signals, store events.
        Returns summary dict.
        """
        from scripts.sync_shortline_us_events import fetch_us_data, detect_signals

        if tickers is None:
            c = self.conn.cursor()
            tickers = [
                r[0] for r in c.execute(
                    "SELECT DISTINCT us_ticker FROM cross_market_mapping_master WHERE enabled=1"
                ).fetchall()
            ]

        df = fetch_us_data(tickers, days=days)
        events = detect_signals(df, tickers)

        inserted = 0
        for ev in events:
            try:
                self.conn.execute(
                    """INSERT OR IGNORE INTO cross_market_signal_events
                       (us_ticker, event_type, event_date, severity, detail_json)
                       VALUES (?, ?, ?, ?, ?)""",
                    (ev["us_ticker"], ev["event_type"], ev["event_date"], ev["severity"], ev["detail_json"]),
                )
                inserted += 1
            except Exception:
                pass
        self.conn.commit()
        return {"tickers_scanned": len(tickers), "events_detected": len(events), "events_inserted": inserted}

    # -- build candidates --
    def build_candidates(self) -> dict:
        """
        Join mapping_master + signal_events to generate execution_priority candidates.
        Returns summary dict.
        """
        c = self.conn.cursor()
        rows = c.execute(
            """SELECT m.us_ticker, m.cn_code, m.cn_name, m.sector, m.confidence, m.signal_type, m.rationale as mapping_rationale,
                      e.event_type, e.event_date, e.severity as event_severity, e.detail_json
               FROM cross_market_mapping_master m
               JOIN cross_market_signal_events e ON m.us_ticker = e.us_ticker
               WHERE m.enabled = 1"""
        ).fetchall()

        inserted = 0
        for r in rows:
            (us_ticker, cn_code, cn_name, sector, confidence, signal_type, mapping_rationale,
             event_type, event_date, event_severity, detail_json) = r
            # execution_priority = confidence * 0.5 + severity * 0.3 + style_bonus * 0.2
            style_bonus = {
                "price_breakout": 1.0,
                "etf_breakout": 0.9,
                "sector_rotation": 0.7,
                "earnings_spillover": 0.8,
            }.get(event_type, 0.5)
            execution_priority = round(confidence * 0.5 + event_severity * 0.3 + style_bonus * 0.2, 4)
            rationale = f"{mapping_rationale} | {event_type}(sev={event_severity})"
            try:
                self.conn.execute(
                    """INSERT OR IGNORE INTO cross_market_signal_candidates
                       (us_ticker, cn_code, cn_name, sector, event_type, event_date, execution_priority, rationale)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (us_ticker, cn_code, cn_name, sector, event_type, event_date, execution_priority, rationale),
                )
                inserted += 1
            except Exception:
                pass
        self.conn.commit()
        return {"rows_joined": len(rows), "candidates_inserted": inserted}
