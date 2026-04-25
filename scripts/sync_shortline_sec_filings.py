"""
Sync SEC filings for US watchlist tickers -> cross_market_signal_events.

Pulls recent filings from data.sec.gov official API, filters 8-K/6-K/10-Q/10-K
events meaningful for short-term trading, and writes structured events to DB.

Usage:
    python -m scripts.sync_shortline_sec_filings
"""
import json
import os
import re
import sqlite3
import time
import urllib.request
from datetime import datetime, timedelta, date

USER_AGENT = "InvestmentControl/1.0 (lhluo; investment-control)"

# ---------------------------------------------------------------------------
# Watchlist & CIK mappings (30 core tickers covering seed sectors)
# ---------------------------------------------------------------------------
WATCHLIST_CIKS = {
    # AI / LLM
    "MSFT": "0000789019", "GOOG": "0001652044", "META": "0001326801",
    "AAPL": "0000320193", "PLTR": "0001321655",
    # 半导体
    "NVDA": "0001045810", "AMD": "0000002488", "AVGO": "0001730168",
    "TSM": "0001046179", "INTC": "0000050863", "QCOM": "0000804328",
    "ARM": "0001946022",
    # 机器人 / EV
    "TSLA": "0001318605",
    # 创新药
    "LLY": "0000059478", "NVO": "0000353278", "MRNA": "0001682852",
    "PFE": "0000078003",
    # 光伏
    "ENPH": "0001463101", "SEDG": "0001652804", "FSLR": "0001274494",
    "CSIQ": "0001375877",
    # 核电
    "CCJ": "0000806819", "VST": "0001045309", "OKLO": "0001973168",
    # 养猪 / 消费
    "TSN": "0000100493", "HRL": "0000048465",
    # 红利
    "XOM": "0000034088", "CVX": "0000093410", "JPM": "0000019617",
    "KO": "0000021344", "PG": "0000080424", "O": "0001037868",
}

# SEC filing types we care about for short-term signals
FILING_TYPES = {"8-K", "6-K", "10-Q", "10-K", "10-K/A", "10-Q/A"}

# ---------------------------------------------------------------------------
# SEC API helpers
# ---------------------------------------------------------------------------

def sec_request(url: str, retries: int = 3) -> dict:
    """Fetch JSON from SEC API with retries and rate-limit pause."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except Exception as e:
            if attempt == retries - 1:
                print(f"  [WARN] SEC fetch failed after {retries} tries: {url} — {e}")
                return {}
            time.sleep(0.5 * (attempt + 1))
    return {}


def get_filing_dates_index() -> dict:
    """Get SEC daily filing index for date->filings mapping.

    We use the RSS feed approach which gives us filingDate directly.
    Falls back to submissions API if RSS is unavailable.
    """
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&count=200&output=atom"
    return {}  # RSS parsing is complex; we use submissions API instead


def fetch_recent_filings_for_cik(cik: str, days: int = 14) -> list[dict]:
    """Fetch recent filings for a CIK from SEC submissions API.

    Returns list of dicts with keys: filing_type, filing_date, accession_number,
    file_number, items, description, size, document_url.
    """
    cik_padded = cik.lstrip("0").zfill(10) if cik else cik
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    data = sec_request(url)

    if not data or "filings" not in data:
        return []

    filings = data.get("filings", [])
    # filings is a list of dicts with: form, filingDate, accessionNumber, fileNumber, ...

    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    results = []

    for f in filings:
        form = f.get("form", "").strip()
        filing_date = f.get("filingDate", "")

        if form not in FILING_TYPES:
            continue
        if filing_date < cutoff:
            continue  # filings are sorted newest-first; we could break but play safe

        accession = f.get("accessionNumber", "").replace("-", "")
        cik_raw = data.get("cik", cik).lstrip("0").zfill(10)
        document_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik_raw}/"
            f"{accession}/{data['name'].lower().replace(' ', '-')}-{accession}.htm"
        )

        results.append({
            "filing_type": form,
            "filing_date": filing_date,
            "accession_number": f.get("accessionNumber", ""),
            "file_number": f.get("fileNumber", ""),
            "items": f.get("items", ""),
            "description": f.get("description", ""),
            "document_url": document_url,
        })

    time.sleep(0.1)  # rate limit
    return results


# ---------------------------------------------------------------------------
# Short-term relevance scoring
# ---------------------------------------------------------------------------

SHORT_ITEMS_8K = {
    "1.01", "1.02",  # Material definitive agreement / termination
    "2.01", "2.02",  # Acquisition/disposition / financial results
    "2.05",          # Cost associated with exit/disposal
    "2.06",          # Impairment / write-off
    "3.01", "3.02",  # Material events / unregistered sales
    "3.03",          # Material modification to rights
    "4.01",          # Change in certifying accountant
    "4.02",          # Non-reliance on previous financials
    "5.02", "5.03",  # Departure of directors / charter amendments
    "5.07",          # Submission to security holder vote
    "5.08",          # Shareholder director nominations
    "6.01", "6.02",  # ABS / material impairment
    "7.01", "8.01",  # Regulation FD / other events
    "9.01",          # Financial statements & exhibits
}

SHORT_ITEMS_6K = {"a", "b", "c"}  # General material events

CN_KEYWORDS = {
    "acquisition": "并购", "disposition": "资产处置", "material agreement": "重大协议",
    "termination": "终止协议", "earnings": "财报", "revenue": "营收",
    "net income": "净利润", "guidance": "业绩指引", "impairment": "减值",
    "write-off": "核销", "bankruptcy": "破产", "restructuring": "重组",
    "ceo": "CEO", "cfo": "CFO", "director": "董事", "officer": "高管",
    "delisting": "退市", "dividend": "分红", "spin-off": "分拆",
    "litigation": "诉讼", "investigation": "调查", "default": "违约",
    "debt": "债务", "financing": "融资", "offering": "增发",
    "warrant": "认股权证", "merger": "合并", "acquisition": "收购",
    "regulation fd": "FD披露", "restatement": "财报重述",
}


def assess_short_term_relevance(filing: dict) -> dict:
    """Score a filing for short-term trading relevance.

    Returns dict with: urgency, severity, headline, summary, should_include.
    """
    form = filing["filing_type"]
    items = filing.get("items", "")
    description = filing.get("description", "").lower()
    text = f"{items} {description}"

    # Urgency by form type
    if form in ("8-K",):
        urgency = "HIGH"
        base_severity = 0.6
    elif form in ("6-K",):
        urgency = "HIGH"
        base_severity = 0.55
    elif form in ("10-Q", "10-Q/A"):
        urgency = "MEDIUM"
        base_severity = 0.5
    elif form in ("10-K", "10-K/A"):
        urgency = "MEDIUM"
        base_severity = 0.45
    else:
        urgency = "LOW"
        base_severity = 0.3

    # Item-level boosters
    item_list = [x.strip() for x in items.replace(",", " ").split()]
    matched_items = [x for x in item_list if x in SHORT_ITEMS_8K]
    item_boost = min(len(matched_items) * 0.08, 0.3)

    # Keyword boost
    kw_hits = [kw for kw in CN_KEYWORDS if kw in text]
    kw_boost = min(len(kw_hits) * 0.05, 0.2)

    severity = round(min(base_severity + item_boost + kw_boost, 1.0), 3)

    # Headline
    if form in ("8-K", "6-K"):
        if matched_items:
            item_desc = ", ".join(matched_items[:3])
            headline = f"{filing.get('ticker', 'UNK')} {form} Items({item_desc})"
        elif kw_hits:
            headline = f"{filing.get('ticker', 'UNK')} {form} {CN_KEYWORDS.get(kw_hits[0], kw_hits[0])}"
        else:
            headline = f"{filing.get('ticker', 'UNK')} {form} filed"
    else:
        headline = f"{filing.get('ticker', 'UNK')} {form} filed ({filing['filing_date']})"

    # Summary
    summary_parts = [f"{form} filed on {filing['filing_date']}."]
    if items:
        summary_parts.append(f" Items: {items}.")
    if kw_hits:
        cn_terms = [CN_KEYWORDS[kw] for kw in kw_hits if kw in CN_KEYWORDS]
        summary_parts.append(f" Key topics: {', '.join(cn_terms[:3])}.")
    summary = "".join(summary_parts)

    # Decision: include if it has meaningful items or keywords, or is 8-K/6-K
    should_include = (
        form in ("8-K", "6-K")
        or len(matched_items) > 0
        or len(kw_hits) > 0
        or severity >= 0.5
    )

    return {
        "urgency": urgency,
        "severity": severity,
        "headline": headline,
        "summary": summary,
        "should_include": should_include,
    }


# ---------------------------------------------------------------------------
# DB integration
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "investment.db")

DDL_ADD_COLUMNS = [
    "ALTER TABLE cross_market_signal_events ADD COLUMN event_id TEXT",
    "ALTER TABLE cross_market_signal_events ADD COLUMN headline TEXT",
    "ALTER TABLE cross_market_signal_events ADD COLUMN summary TEXT",
    "ALTER TABLE cross_market_signal_events ADD COLUMN source_url TEXT",
    "ALTER TABLE cross_market_signal_events ADD COLUMN source_tier TEXT DEFAULT 'T0'",
    "ALTER TABLE cross_market_signal_events ADD COLUMN urgency TEXT DEFAULT 'MEDIUM'",
]


def ensure_columns(conn: sqlite3.Connection):
    """Add new columns if they don't exist (SQLite <3.35 doesn't support IF NOT EXISTS for columns)."""
    existing = {
        r[1] for r in conn.execute("PRAGMA table_info(cross_market_signal_events)").fetchall()
    }
    for ddl in DDL_ADD_COLUMNS:
        col = ddl.split("ADD COLUMN ")[1].split()[0]
        if col not in existing:
            try:
                conn.execute(ddl)
            except sqlite3.OperationalError:
                pass  # column may have been added concurrently
    conn.commit()


def generate_event_id(ticker: str, form: str, filing_date: str) -> str:
    """Generate a deterministic event ID."""
    return f"SEC-{ticker}-{form}-{filing_date}"


def upsert_event(conn: sqlite3.Connection, ticker: str, form: str, filing_date: str,
                 severity: float, headline: str, summary: str, source_url: str,
                 urgency: str) -> bool:
    """Insert or ignore a filing event. Returns True if inserted."""
    event_id = generate_event_id(ticker, form, filing_date)
    detail = json.dumps({
        "event_id": event_id,
        "source_tier": "T0",
        "urgency": urgency,
    }, ensure_ascii=False)

    try:
        conn.execute(
            """INSERT OR IGNORE INTO cross_market_signal_events
               (us_ticker, event_type, event_date, severity, detail_json,
                source, headline, summary, source_url, source_tier, urgency, event_id)
               VALUES (?, ?, ?, ?, ?, 'sec_edgar', ?, ?, ?, 'T0', ?, ?)""",
            (ticker, form, filing_date, severity, detail,
             headline, summary, source_url, urgency, event_id),
        )
        conn.commit()
        return True
    except Exception as e:
        print(f"  [WARN] Insert failed for {ticker}/{form}: {e}")
        return False


# ---------------------------------------------------------------------------
# Main sync
# ---------------------------------------------------------------------------

def sync(
    db_path: str = DB_PATH,
    days: int = 14,
    tickers: list[str] | None = None,
    dry_run: bool = False,
) -> dict:
    """Main entry: fetch SEC filings, score, and write events.

    Returns summary dict with scan stats.
    """
    from app.services.shortline_service import ensure_tables

    conn = ensure_tables(db_path)
    ensure_columns(conn)

    target_tickers = tickers or list(WATCHLIST_CIKS.keys())
    stats = {
        "tickers_scanned": 0,
        "filings_found": 0,
        "filings_relevant": 0,
        "events_inserted": 0,
        "events_skipped_dup": 0,
        "errors": 0,
        "run_at": datetime.now().isoformat(),
    }

    print(f"=== SEC Filing Sync | {datetime.now().strftime('%Y-%m-%d %H:%M')} | "
          f"watchlist={len(target_tickers)} | days={days} ===\n")

    for ticker in target_tickers:
        cik = WATCHLIST_CIKS.get(ticker)
        if not cik:
            print(f"  [SKIP] {ticker}: no CIK mapping")
            continue

        stats["tickers_scanned"] += 1
        print(f"\n[{ticker}] CIK={cik} ...")

        filings = fetch_recent_filings_for_cik(cik, days=days)
        stats["filings_found"] += len(filings)

        if not filings:
            print(f"  (no relevant filings in {days}d)")
            continue

        for filing in filings:
            filing["ticker"] = ticker
            assessment = assess_short_term_relevance(filing)

            if not assessment["should_include"]:
                print(f"  [FILTER] {filing['filing_type']} {filing['filing_date']} "
                      f"— low short-term relevance")
                continue

            stats["filings_relevant"] += 1

            if dry_run:
                print(f"  [DRY-RUN] {assessment['headline']} | "
                      f"sev={assessment['severity']} | "
                      f"urgency={assessment['urgency']}")
                continue

            inserted = upsert_event(
                conn, ticker, filing["filing_type"], filing["filing_date"],
                assessment["severity"], assessment["headline"],
                assessment["summary"], filing["document_url"],
                assessment["urgency"],
            )
            if inserted:
                stats["events_inserted"] += 1
                print(f"  [NEW] {assessment['headline']} | "
                      f"sev={assessment['severity']} | "
                      f"urgency={assessment['urgency']}")
            else:
                stats["events_skipped_dup"] += 1
                print(f"  [DUP] {filing['filing_type']} {filing['filing_date']}")

    print(f"\n=== Summary ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    conn.close()
    return stats


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sync SEC filings -> cross_market_signal_events")
    parser.add_argument("--days", type=int, default=14, help="Lookback days (default 14)")
    parser.add_argument("--tickers", nargs="*", help="Specific tickers to scan (default: all watchlist)")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing to DB")
    args = parser.parse_args()
    sync(days=args.days, tickers=args.tickers, dry_run=args.dry_run)
