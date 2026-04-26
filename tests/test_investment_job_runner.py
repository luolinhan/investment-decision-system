from __future__ import annotations

import json
import sys
from pathlib import Path

from scripts.investment_job_runner import (
    InvestmentJobRunner,
    default_variables,
    extract_metrics,
    load_manifest,
)


def write_manifest(path: Path) -> Path:
    payload = {
        "version": 1,
        "tasks": {
            "emit_metrics": {
                "description": "Emit a synthetic ETL metric line.",
                "command": [
                    "{python}",
                    "-c",
                    "import json; print('ETL_METRICS_JSON=' + json.dumps({{'records_processed': 3, 'records_failed': 0}}))",
                ],
                "cwd": "{repo_root}",
                "timeout_seconds": 10,
            },
            "echo_limit": {
                "command": ["{python}", "-c", "import os; print(os.environ.get('TEST_LIMIT'))"],
                "cwd": "{repo_root}",
                "env": {"TEST_LIMIT": "{limit}"},
            },
        },
        "sequences": {
            "daily": {
                "steps": [{"task": "emit_metrics"}, {"task": "echo_limit"}],
            }
        },
        "defaults": {"limit": "5"},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_load_manifest_validates_tasks_and_sequences(tmp_path: Path):
    manifest = load_manifest(write_manifest(tmp_path / "manifest.json"))

    assert sorted(manifest["tasks"]) == ["echo_limit", "emit_metrics"]
    assert manifest["sequences"]["daily"]["steps"][0]["task"] == "emit_metrics"


def test_extract_metrics_reads_last_metric_line():
    text = "noise\nETL_METRICS_JSON={\"records_processed\": 1}\nETL_METRICS_JSON={\"records_processed\": 2}\n"

    assert extract_metrics(text) == {"records_processed": 2}


def test_runner_dry_run_sequence_writes_plan(tmp_path: Path):
    manifest = load_manifest(write_manifest(tmp_path / "manifest.json"))
    runner = InvestmentJobRunner(
        manifest,
        repo_root=tmp_path,
        log_dir=tmp_path / "logs",
        lock_dir=tmp_path / "locks",
        variables=default_variables(tmp_path, {"python": sys.executable, "limit": "9"}),
        dry_run=True,
    )

    result = runner.run_sequence("daily", skip={"echo_limit"})

    assert result["status"] == "success"
    assert [record["status"] for record in result["tasks"]] == ["planned", "skipped"]
    assert list((tmp_path / "logs").glob("*.jsonl"))


def test_runner_executes_task_and_captures_metrics(tmp_path: Path):
    manifest = load_manifest(write_manifest(tmp_path / "manifest.json"))
    runner = InvestmentJobRunner(
        manifest,
        repo_root=tmp_path,
        log_dir=tmp_path / "logs",
        lock_dir=tmp_path / "locks",
        variables=default_variables(tmp_path, {"python": sys.executable, "limit": "9"}),
        no_lock=True,
    )

    result = runner.run_task("emit_metrics")

    assert result["status"] == "success"
    assert result["metrics"] == {"records_processed": 3, "records_failed": 0}
    assert Path(result["stdout_log"]).exists()
    assert Path(result["stderr_log"]).exists()
