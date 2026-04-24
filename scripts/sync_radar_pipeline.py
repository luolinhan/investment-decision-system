#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Radar 数据统一同步入口。

顺序：
1. 指标目录
2. 宏观 / 外部
3. 官方宏观补数（若存在）
4. 赛道与情报派生信号
5. 香港流动性 / 微观结构
6. Pentagon Pizza
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List


BASE_DIR = Path(__file__).resolve().parents[1]
PYTHON = sys.executable or "python"

PIPELINE = [
    {"key": "catalog", "path": BASE_DIR / "scripts" / "sync_radar_catalog.py", "required": True},
    {"key": "macro_external", "path": BASE_DIR / "scripts" / "sync_radar_macro_external.py", "required": True},
    {"key": "macro_official", "path": BASE_DIR / "scripts" / "sync_radar_macro_official_snapshots.py", "required": False},
    {"key": "sector", "path": BASE_DIR / "scripts" / "sync_radar_sector.py", "required": False},
    {"key": "intelligence_signals", "path": BASE_DIR / "scripts" / "sync_radar_intelligence_signals.py", "required": False},
    {"key": "hk", "path": BASE_DIR / "scripts" / "sync_radar_hk.py", "required": False},
    {"key": "hk_microstructure", "path": BASE_DIR / "scripts" / "sync_radar_hk_microstructure.py", "required": False},
    {"key": "pizza", "path": BASE_DIR / "scripts" / "sync_pentagon_pizza_history.py", "required": False},
]


def parse_metrics(output: str) -> Dict[str, object]:
    for line in reversed((output or "").splitlines()):
        if line.startswith("ETL_METRICS_JSON="):
            try:
                return json.loads(line.split("=", 1)[1])
            except Exception:
                return {}
    return {}


def run_step(step: Dict[str, object]) -> Dict[str, object]:
    path = Path(step["path"])
    if not path.exists():
        status = "missing_required" if step.get("required") else "skipped"
        return {
            "key": step["key"],
            "path": str(path),
            "status": status,
            "returncode": None,
            "metrics": {},
            "stdout_tail": "",
        }

    proc = subprocess.run(
        [PYTHON, str(path)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        timeout=1800,
    )
    combined = "\n".join(filter(None, [proc.stdout, proc.stderr]))
    return {
        "key": step["key"],
        "path": str(path),
        "status": "ok" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "metrics": parse_metrics(combined),
        "stdout_tail": "\n".join(combined.splitlines()[-20:]),
    }


def main() -> int:
    print("=" * 60)
    print(f"Radar Pipeline Sync - {datetime.now()}")
    print("=" * 60)
    results: List[Dict[str, object]] = []
    for step in PIPELINE:
        print(f"\n[STEP] {step['key']} -> {step['path']}")
        result = run_step(step)
        results.append(result)
        print(f"  status={result['status']} returncode={result['returncode']}")
        if result.get("metrics"):
            print(f"  metrics={json.dumps(result['metrics'], ensure_ascii=False)}")
        tail = result.get("stdout_tail") or ""
        if tail:
            print("  --- output tail ---")
            print(tail)

    failures = [item for item in results if item["status"] in {"failed", "missing_required"}]
    print("\nRADAR_PIPELINE_JSON=" + json.dumps({
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "results": results,
        "failure_count": len(failures),
    }, ensure_ascii=False))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
