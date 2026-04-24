# -*- coding: utf-8 -*-
"""Sync intelligence-derived sector signals into the radar DuckDB store.

Reads SQLite data/investment.db (intelligence_events / research_reports)
and produces daily time-series indicators for six radar sectors:

  Indicator code                       | Sector thesis
  ─────────────────────────────────────┼─────────────────────────────
  SECTOR_AI_MODEL_COMPETITIVENESS      | AI model competitiveness
  SECTOR_AI_CAPEX                      | AI capital-expenditure sentiment
  SECTOR_PHARMA_BD_VALUE               | Pharma BD heat (score proxy, not amounts)
  SECTOR_PHARMA_CLINICAL_MILESTONES    | Pharma clinical milestone activity
  SECTOR_SEMI_DOMESTIC_SUB             | Semiconductor domestic substitution
  SECTOR_PV_SUPPLY_DISCIPLINE          | PV / solar supply-side discipline

Scoring methodology:
  Each indicator is a weighted score computed from intelligence events
  whose title/summary/thesis matches predefined keyword sets.

  score(date) = min(100, sum over events:
    priority_weight(priority) * recency_weight(days_ago))

  Priority weights:  P0 → 3.0,  P1 → 2.0,  P2 → 1.0,  default → 0.5
  Recency weights:   0 days → 1.00,  7 days → 0.50,
                     30 days → 0.15,  90 days → 0.05,  else → 0.01

  The time series is "forward-filled": once a date gets a score, every
  subsequent calendar day until the next scored date carries the same value.
  This produces a continuous daily series rather than sparse event dates.

  For SECTOR_PHARMA_BD_VALUE the value is a BD heat score proxy —
  NOT real deal amounts.  This is documented in notes and source.

Empty-intelligence handling:
  If the SQLite DB does not exist, is empty, or lacks the expected tables,
  the script records a source_run for each indicator with status="partial"
  and exits cleanly (exit code 0).  No exception is raised.

Run:
    python scripts/sync_radar_intelligence_signals.py
"""
import logging
import os
import sqlite3
import sys
import traceback
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.radar_store import RadarStore

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQLITE_PATH = os.getenv(
    "INVESTMENT_DB_PATH",
    os.path.join(BASE_DIR, "data", "investment.db"),
)

# ---------------------------------------------------------------------------
# Indicator definitions
# ---------------------------------------------------------------------------

INDICATORS: List[Dict[str, Any]] = [
    {
        "indicator_code": "SECTOR_AI_MODEL_COMPETITIVENESS",
        "category": "sector",
        "indicator_type": "score",
        "frequency": "daily",
        "direction": "leading",
        "half_life_days": 14,
        "affected_assets": ["AI infrastructure", "cloud providers", "GPU supply chain"],
        "affected_sectors": ["AI", "semiconductors"],
        "source": "intelligence_events:keyword_score",
        "confidence": 0.70,
        "status": "active",
        "notes": "AI model competitiveness: weighted keyword score from P0–P2 events in ai_model category. Keywords: frontier model names, benchmark, agent, inference, launch, release.",
    },
    {
        "indicator_code": "SECTOR_AI_CAPEX",
        "category": "sector",
        "indicator_type": "score",
        "frequency": "daily",
        "direction": "leading",
        "half_life_days": 21,
        "affected_assets": ["data-center REITs", "server OEMs", "power/cooling"],
        "affected_sectors": ["AI infrastructure", "data centers"],
        "source": "intelligence_events:keyword_score",
        "confidence": 0.65,
        "status": "active",
        "notes": "AI capex sentiment: keyword score from events mentioning data-center, capex, GPU, server, cloud workload, training cluster.",
    },
    {
        "indicator_code": "SECTOR_PHARMA_BD_VALUE",
        "category": "sector",
        "indicator_type": "score",
        "frequency": "daily",
        "direction": "leading",
        "half_life_days": 30,
        "affected_assets": ["HK/A biotech", "CRO/CDMO"],
        "affected_sectors": ["biotech", "innovative drugs"],
        "source": "intelligence_events:keyword_score",
        "confidence": 0.55,
        "status": "active",
        "notes": "Pharma BD heat score PROXY — not real deal amounts. Counts licensing, partnership, co-development, royalty events weighted by priority and recency. Value is a 0–100 score.",
    },
    {
        "indicator_code": "SECTOR_PHARMA_CLINICAL_MILESTONES",
        "category": "sector",
        "indicator_type": "score",
        "frequency": "daily",
        "direction": "leading",
        "half_life_days": 21,
        "affected_assets": ["HK/A biotech"],
        "affected_sectors": ["biotech", "clinical-stage"],
        "source": "intelligence_events:keyword_score",
        "confidence": 0.60,
        "status": "active",
        "notes": "Pharma clinical milestone activity: keyword score from events mentioning approval, Phase I/II/III, NDA, BLA, clinical trial, readout.",
    },
    {
        "indicator_code": "SECTOR_SEMI_DOMESTIC_SUB",
        "category": "sector",
        "indicator_type": "score",
        "frequency": "daily",
        "direction": "leading",
        "half_life_days": 30,
        "affected_assets": ["A-share semiconductors", "domestic EDA/IP"],
        "affected_sectors": ["semiconductors", "domestic substitution"],
        "source": "intelligence_events:keyword_score",
        "confidence": 0.60,
        "status": "active",
        "notes": "Semiconductor domestic substitution: keyword score from events mentioning domestic, substitution, localization, 国产, 替代, chip, fab, foundry, SMIC, Huawei chip.",
    },
    {
        "indicator_code": "SECTOR_PV_SUPPLY_DISCIPLINE",
        "category": "sector",
        "indicator_type": "score",
        "frequency": "daily",
        "direction": "leading",
        "half_life_days": 30,
        "affected_assets": ["PV manufacturers", "solar ETFs"],
        "affected_sectors": ["solar", "photovoltaic"],
        "source": "intelligence_events:keyword_score",
        "confidence": 0.55,
        "status": "active",
        "notes": "PV supply discipline: keyword score from events mentioning solar, photovoltaic, PV, 光伏, capacity cut, production cut, price floor, supply constraint.",
    },
]

# ---------------------------------------------------------------------------
# Keyword sets per indicator
# ---------------------------------------------------------------------------

KEYWORD_MAP: Dict[str, Tuple[str, ...]] = {
    "SECTOR_AI_MODEL_COMPETITIVENESS": (
        "gpt", "claude", "gemini", "llama", "deepseek", "qwen", "mistral",
        "grok", "frontier model", "benchmark", "agent", "agents",
        "inference", "launch", "release", "introducing", "model capability",
        "reasoning model", "multimodal", "context window", "token",
        "AI model", "large language model", "LLM",
    ),
    "SECTOR_AI_CAPEX": (
        "data center", "data-center", "capex", "capital expenditure",
        "GPU", "H100", "H200", "B200", "GB200", "training cluster",
        "server", "cloud workload", "compute", "nvidia", "TSMC",
        "inference cost", "AI infrastructure", "data-center capex",
        "AI server", "optical module", "PCB", "memory chip",
    ),
    "SECTOR_PHARMA_BD_VALUE": (
        "licensing", "license agreement", "partnership", "co-development",
        "royalty", "milestone payment", "deal", "acquisition",
        "business development", "BD", "collaboration", "in-license",
        "out-license", "exclusive", "non-exclusive", "strategic partnership",
        "authorization", "commercialization",
    ),
    "SECTOR_PHARMA_CLINICAL_MILESTONES": (
        "Phase I", "Phase II", "Phase III", "Phase 1", "Phase 2", "Phase 3",
        "NDA", "BLA", "IND", "approval", "approved", "FDA",
        "clinical trial", "clinical readout", "clinical data",
        "primary endpoint", "met endpoint", "statistically significant",
        "pivotal", "registration trial", "orphan drug", "breakthrough therapy",
        "fast track",
    ),
    "SECTOR_SEMI_DOMESTIC_SUB": (
        "domestic", "substitution", "localization", "国产", "替代",
        "chip", "fab", "foundry", "SMIC", "Huawei chip", "semiconductor",
        "EDA", "IP core", "lithography", "ASML", "chip act",
        "export control", "sanction", "self-reliance", "自主可控",
        "先进工艺", "成熟工艺", "封装", "先进封装",
    ),
    "SECTOR_PV_SUPPLY_DISCIPLINE": (
        "solar", "photovoltaic", "PV", "光伏",
        "capacity cut", "production cut", "price floor", "supply constraint",
        "overcapacity", "utilization rate", "polysilicon", "wafer",
        "cell price", "module price", "tier 1", "involution",
        "产能出清", "限产", "减产", "涨价",
    ),
}

# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

PRIORITY_WEIGHTS = {"P0": 3.0, "P1": 2.0, "P2": 1.0}


def _priority_weight(priority: Optional[str]) -> float:
    return PRIORITY_WEIGHTS.get(priority, 0.5)


def _recency_weight(days_ago: int) -> float:
    """Exponential-ish decay: recent events matter more."""
    if days_ago <= 0:
        return 1.0
    if days_ago <= 7:
        return 0.50
    if days_ago <= 30:
        return 0.15
    if days_ago <= 90:
        return 0.05
    return 0.01


def _score_event(priority: Optional[str], event_date: Optional[str], ref_date: datetime) -> float:
    """Return the weighted contribution of a single event."""
    pw = _priority_weight(priority)
    if event_date:
        try:
            ev_dt = datetime.strptime(event_date[:10], "%Y-%m-%d")
            days_ago = (ref_date - ev_dt).days
        except (ValueError, TypeError):
            days_ago = 365  # treat unparseable dates as old
    else:
        days_ago = 365
    return pw * _recency_weight(days_ago)


def _contains_any(text: str, terms: Tuple[str, ...]) -> bool:
    text_lower = text.lower()
    return any(term.lower() in text_lower for term in terms)


# ---------------------------------------------------------------------------
# Data extraction from SQLite
# ---------------------------------------------------------------------------


def _open_sqlite(path: str) -> Optional[sqlite3.Connection]:
    """Open SQLite connection; return None if DB missing or empty."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    try:
        conn = sqlite3.connect(path)
        # Verify tables exist
        tables = [
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        if "intelligence_events" not in tables:
            conn.close()
            return None
        return conn
    except Exception:
        return None


def fetch_relevant_events(
    conn: sqlite3.Connection,
) -> List[Dict[str, Any]]:
    """
    Pull events that match ANY keyword set, along with their text fields.
    We fetch broadly and filter in Python to keep the query simple.
    """
    # Pull from intelligence_events
    events: List[Dict[str, Any]] = []
    try:
        rows = conn.execute(
            """
            SELECT event_key, category, priority, event_time,
                   title, title_zh, summary, summary_zh,
                   impact_score, verification_status
            FROM intelligence_events
            WHERE status = 'active'
            ORDER BY event_time DESC
            """
        ).fetchall()
        for row in rows:
            events.append({
                "event_key": row[0],
                "category": row[1],
                "priority": row[2],
                "event_time": row[3],
                "title": row[4] or "",
                "title_zh": row[5] or "",
                "summary": row[6] or "",
                "summary_zh": row[7] or "",
                "impact_score": row[8],
                "verification_status": row[9],
                "source_table": "intelligence_events",
            })
    except Exception as e:
        logger.warning("Failed to read intelligence_events: %s", e)

    # Pull from research_reports — these carry thesis text that is valuable
    try:
        rows = conn.execute(
            """
            SELECT report_key, source_key, published_at,
                   title, title_zh, summary, summary_zh,
                   thesis, thesis_zh, relevance, relevance_zh
            FROM research_reports
            WHERE status = 'active'
            ORDER BY published_at DESC
            """
        ).fetchall()
        for row in rows:
            events.append({
                "event_key": row[0],
                "category": row[1],
                "priority": "P2",  # research reports default to P2
                "event_time": row[2],
                "title": row[3] or "",
                "title_zh": row[4] or "",
                "summary": row[5] or "",
                "summary_zh": row[6] or "",
                "impact_score": None,
                "verification_status": None,
                "source_table": "research_reports",
                "thesis": row[7] or "",
                "thesis_zh": row[8] or "",
                "relevance": row[9] or "",
                "relevance_zh": row[10] or "",
            })
    except Exception as e:
        logger.warning("Failed to read research_reports: %s", e)

    return events


# ---------------------------------------------------------------------------
# Score computation
# ---------------------------------------------------------------------------


def _aggregate_text(event: Dict[str, Any]) -> str:
    """Combine all text fields for keyword matching."""
    parts = [
        event.get("title", ""),
        event.get("title_zh", ""),
        event.get("summary", ""),
        event.get("summary_zh", ""),
        event.get("thesis", ""),
        event.get("thesis_zh", ""),
        event.get("relevance", ""),
        event.get("relevance_zh", ""),
    ]
    return " ".join(p for p in parts if p)


def compute_indicator_series(
    events: List[Dict[str, Any]],
    indicator_code: str,
    today: datetime,
    lookback_days: int = 180,
) -> List[Tuple[str, float]]:
    """
    Compute a daily forward-filled time series for the given indicator.

    Returns list of (date_str, score) sorted ascending by date.
    """
    keywords = KEYWORD_MAP.get(indicator_code, ())
    if not keywords:
        return []

    # Step 1: find matching events and compute their scores
    scored_events: List[Tuple[str, float]] = []
    for ev in events:
        text = _aggregate_text(ev)
        if _contains_any(text, keywords):
            ev_date = ev.get("event_time")
            score = _score_event(ev.get("priority"), ev_date, today)
            if ev_date:
                date_str = ev_date[:10]
            else:
                date_str = today.strftime("%Y-%m-%d")
            scored_events.append((date_str, score))

    if not scored_events:
        return []

    # Step 2: aggregate scores by date
    date_scores: Dict[str, float] = {}
    for ds, sc in scored_events:
        date_scores[ds] = date_scores.get(ds, 0.0) + sc

    # Step 3: cap at 100 and build sorted date list
    all_dates = sorted(date_scores.keys())
    capped = {d: min(100.0, round(s, 2)) for d, s in date_scores.items()}

    # Step 4: forward-fill to produce daily series
    start_date = today - timedelta(days=lookback_days)
    series: List[Tuple[str, float]] = []
    current_score = 0.0
    date_idx = 0
    n_dates = len(all_dates)

    for i in range(lookback_days + 1):
        d = start_date + timedelta(days=i)
        d_str = d.strftime("%Y-%m-%d")
        # Advance through scored dates up to and including today
        while date_idx < n_dates and all_dates[date_idx] <= d_str:
            current_score = capped[all_dates[date_idx]]
            date_idx += 1
        series.append((d_str, round(current_score, 2)))

    return series


# ---------------------------------------------------------------------------
# Catalog & observation writing
# ---------------------------------------------------------------------------


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_catalog(store: RadarStore) -> None:
    """Upsert indicator catalog entries that don't exist or are stale."""
    conn = store.get_connection()
    try:
        for ind in INDICATORS:
            existing = conn.execute(
                "SELECT last_update FROM indicator_catalog WHERE indicator_code = ?",
                (ind["indicator_code"],),
            ).fetchone()
            if existing is None:
                import json
                affected_assets = ind.get("affected_assets")
                if isinstance(affected_assets, list):
                    affected_assets = json.dumps(affected_assets, ensure_ascii=False)
                affected_sectors = ind.get("affected_sectors")
                if isinstance(affected_sectors, list):
                    affected_sectors = json.dumps(affected_sectors, ensure_ascii=False)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO indicator_catalog
                    (indicator_code, category, indicator_type, frequency,
                     direction, half_life_days, affected_assets, affected_sectors,
                     source, confidence, last_update, status, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ind["indicator_code"],
                        ind.get("category"),
                        ind.get("indicator_type"),
                        ind.get("frequency"),
                        ind.get("direction"),
                        ind.get("half_life_days"),
                        affected_assets,
                        affected_sectors,
                        ind.get("source"),
                        ind.get("confidence"),
                        _now_utc(),
                        ind.get("status", "active"),
                        ind.get("notes"),
                    ),
                )
            else:
                conn.execute(
                    "UPDATE indicator_catalog SET last_update = ? WHERE indicator_code = ?",
                    (_now_utc(), ind["indicator_code"]),
                )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------


def _collect_one_indicator(
    store: RadarStore,
    indicator_code: str,
    series: List[Tuple[str, float]],
    notes_suffix: str = "",
) -> Dict[str, Any]:
    """Upsert a single indicator's daily series and record source run."""
    started = _now_utc()
    rows_upserted = 0
    obs_rows: List[Dict[str, Any]] = []

    for date_str, score in series:
        obs_rows.append({
            "indicator_code": indicator_code,
            "obs_date": date_str,
            "value": score,
            "unit": "score_0_100",
            "source": f"intelligence_events:keyword_score:{indicator_code}",
            "quality_flag": "estimated",
            "notes": f"Weighted keyword score from intelligence events. {notes_suffix}",
        })

    if obs_rows:
        rows_upserted = store.upsert_indicator_observations(obs_rows)

    total_read = len(series)
    if total_read > 0:
        status = "success"
        notes = f"{len(series)} daily observations, latest score={series[-1][1]}"
    else:
        status = "partial"
        notes = "No matching events found in intelligence database"

    if notes_suffix:
        notes = f"{notes} — {notes_suffix}"

    store.record_source_run(
        source_name=f"sync_radar_intelligence:{indicator_code}",
        target_table="indicator_observations",
        started_at=started,
        finished_at=_now_utc(),
        status=status,
        rows_read=total_read,
        rows_upserted=rows_upserted,
        notes=notes,
    )
    return {
        "started_at": started,
        "finished_at": _now_utc(),
        "status": status,
        "rows_read": total_read,
        "rows_upserted": rows_upserted,
        "notes": notes,
    }


def collect_intelligence_signals(store: RadarStore) -> List[Dict[str, Any]]:
    """
    Main collection entry point.
    Reads SQLite, computes all indicator series, writes to DuckDB.
    Returns a list of result dicts (one per indicator).
    """
    today = datetime.now(timezone.utc)

    # --- Open SQLite ---
    sqlite_conn = _open_sqlite(SQLITE_PATH)
    if sqlite_conn is None:
        logger.info(
            "[EMPTY] SQLite intelligence DB not available at %s — recording empty source runs",
            SQLITE_PATH,
        )
        results = []
        for ind in INDICATORS:
            results.append(
                _collect_one_indicator(
                    store,
                    ind["indicator_code"],
                    [],
                    notes_suffix="Intelligence database empty or missing",
                )
            )
        return results

    try:
        events = fetch_relevant_events(sqlite_conn)
    finally:
        sqlite_conn.close()

    logger.info("Fetched %d total events/reports from SQLite", len(events))

    # --- Compute and write each indicator ---
    results = []
    for ind in INDICATORS:
        code = ind["indicator_code"]
        series = compute_indicator_series(events, code, today)
        result = _collect_one_indicator(
            store,
            code,
            series,
            notes_suffix=f"{len(series)} days scored from {len(events)} source events",
        )
        results.append(result)
        logger.info(
            "[%s] %s: %d obs, score range [%.1f – %.1f]",
            result["status"].upper(),
            code,
            result["rows_read"],
            min((s[1] for s in series), default=0),
            max((s[1] for s in series), default=0),
        )

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    started_at = _now_utc()
    print("=" * 60)
    print(f"Radar Intelligence Signals Sync – {started_at}")
    print("=" * 60)

    store = RadarStore()
    store.ensure_schema()
    _ensure_catalog(store)

    print(f"\nSQLite source: {SQLITE_PATH}")
    print(f"Indicators: {len(INDICATORS)}")

    results = collect_intelligence_signals(store)

    # Summary
    print(f"\n{'=' * 60}")
    print("Summary:")
    total_ok = sum(1 for r in results if r.get("status") == "success")
    total_partial = sum(1 for r in results if r.get("status") == "partial")
    total_obs = sum(r.get("rows_upserted", 0) for r in results)
    print(f"  Success: {total_ok}  |  Partial: {total_partial}")
    print(f"  Total observations upserted: {total_obs}")
    for r in results:
        print(f"  {r['notes']}")
    print("=" * 60)
    print(
        "ETL_METRICS_JSON="
        + json.dumps(
            {
                "generated_at": datetime.now().replace(microsecond=0).isoformat(),
                "indicator_count": len(INDICATORS),
                "success_count": total_ok,
                "partial_count": total_partial,
                "rows_upserted": total_obs,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
