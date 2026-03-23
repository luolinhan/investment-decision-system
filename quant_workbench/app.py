"""
独立端口量化工作台
"""
from __future__ import annotations

import logging
import subprocess
import sys
from contextlib import asynccontextmanager
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request

from quant_workbench.config import APP_PORT, BASE_DIR, LOG_PATH
from quant_workbench.service import QuantWorkbenchService


LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_PATH, encoding="utf-8")],
)
logger = logging.getLogger(__name__)

service = QuantWorkbenchService()
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
# ensure fallback search path inside the package in case the root templates directory isn't accessible
templates.env.loader.searchpath.append(str(Path(__file__).resolve().parent / "templates"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("启动量化工作台...")
    yield
    logger.info("关闭量化工作台...")


app = FastAPI(
    title="Quant Workbench",
    description="多维共振机会工作台",
    version="0.1.0",
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("quant_workbench.html", {"request": request})


def _summarize_opportunities(opportunities: List[Dict[str, Any]]) -> Dict[str, Any]:
    grade_counts = Counter(item.get("grade", "C") for item in opportunities)
    total_score = sum(float(item.get("score", 0) or 0) for item in opportunities)
    top_candidates = opportunities[:5]
    best = opportunities[0] if opportunities else None

    return {
        "total_candidates": len(opportunities),
        "grade_counts": {
            "A": grade_counts.get("A", 0),
            "B": grade_counts.get("B", 0),
            "C": grade_counts.get("C", 0),
        },
        "average_score": round(total_score / len(opportunities), 1) if opportunities else 0,
        "top_score": best.get("score") if best else 0,
        "top_name": best.get("name") if best else None,
        "top_candidates": top_candidates,
        "risk_flagged": sum(1 for item in opportunities if item.get("risk_flags")),
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "port": APP_PORT}


@app.get("/api/overview")
async def overview():
    opportunities = service.list_opportunities()
    summary = _summarize_opportunities(opportunities)
    status = service.get_status()
    return {
        "status": status,
        "market_regime": service.get_market_regime(),
        "summary": summary,
        "opportunities": opportunities,
        "top_candidates": summary["top_candidates"],
        "generated_at": status.get("last_sync_at"),
    }


@app.get("/api/opportunities")
async def opportunities(limit: int = 20, grade: str = "", market: str = ""):
    items = service.list_opportunities()
    if grade:
        items = [item for item in items if item.get("grade") == grade.upper()]
    if market:
        items = [item for item in items if item.get("market") == market.upper()]
    if limit > 0:
        items = items[:limit]

    return {
        "summary": _summarize_opportunities(items),
        "items": items,
    }


@app.get("/api/stocks/{code}")
async def stock_detail(code: str):
    try:
        return service.get_stock_detail(code)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"unknown stock: {code}") from exc


@app.get("/api/opportunities/{code}")
async def opportunity_detail(code: str):
    return await stock_detail(code)


@app.post("/api/admin/refresh")
async def refresh():
    script = BASE_DIR / "sync_quant_workbench.py"
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        timeout=600,
    )
    status = service.get_status()
    return {
        "ok": result.returncode == 0,
        "returncode": result.returncode,
        "stdout": result.stdout[-4000:],
        "stderr": result.stderr[-4000:],
        "status": status,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("quant_workbench.app:app", host="0.0.0.0", port=APP_PORT, reload=False)
