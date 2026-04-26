#!/usr/bin/env python3
"""Unified local runner for Investment Hub maintenance jobs.

The runner gives scripts a consistent contract without changing the database:
manifest-defined commands, non-overlap locks, JSONL run logs, dry-run planning,
stdout/stderr capture, and simple ETL metric extraction.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = REPO_ROOT / "config" / "job_manifest.example.json"
DEFAULT_LOG_DIR = REPO_ROOT / "logs" / "job-runs"
DEFAULT_LOCK_DIR = REPO_ROOT / "tmp" / "job-locks"


class JobRunnerError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def load_manifest(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise JobRunnerError(f"manifest not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise JobRunnerError("manifest root must be an object")
    validate_manifest(payload)
    return payload


def validate_manifest(manifest: Dict[str, Any]) -> None:
    tasks = manifest.get("tasks")
    if not isinstance(tasks, dict) or not tasks:
        raise JobRunnerError("manifest.tasks must be a non-empty object")
    for name, task in tasks.items():
        if not isinstance(task, dict):
            raise JobRunnerError(f"task {name} must be an object")
        command = task.get("command")
        if not isinstance(command, list) or not command or not all(isinstance(part, str) for part in command):
            raise JobRunnerError(f"task {name}.command must be a non-empty string array")
    sequences = manifest.get("sequences", {})
    if not isinstance(sequences, dict):
        raise JobRunnerError("manifest.sequences must be an object when present")
    for sequence_name, sequence in sequences.items():
        steps = sequence.get("steps") if isinstance(sequence, dict) else None
        if not isinstance(steps, list) or not steps:
            raise JobRunnerError(f"sequence {sequence_name}.steps must be a non-empty array")
        for step in steps:
            task_name = step.get("task") if isinstance(step, dict) else step
            if not isinstance(task_name, str) or task_name not in tasks:
                raise JobRunnerError(f"sequence {sequence_name} references unknown task {task_name}")


def parse_setters(values: Iterable[str]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for raw in values:
        if "=" not in raw:
            raise JobRunnerError(f"--set expects key=value, got {raw}")
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key:
            raise JobRunnerError("--set key cannot be empty")
        result[key] = value
    return result


def default_variables(repo_root: Path, overrides: Dict[str, str]) -> Dict[str, str]:
    variables = {
        "repo_root": str(repo_root),
        "python": sys.executable,
        "date": datetime.now().strftime("%Y-%m-%d"),
    }
    variables.update(overrides)
    return variables


def expand_value(value: str, variables: Dict[str, str]) -> str:
    try:
        return value.format(**variables)
    except KeyError as exc:
        raise JobRunnerError(f"unknown template variable: {exc.args[0]}") from exc


def expand_command(command: List[str], variables: Dict[str, str]) -> List[str]:
    return [expand_value(part, variables) for part in command]


def expand_env(env: Dict[str, Any], variables: Dict[str, str]) -> Dict[str, str]:
    return {str(key): expand_value(str(value), variables) for key, value in (env or {}).items()}


def redact_command(command: List[str]) -> List[str]:
    redacted = []
    for part in command:
        upper = part.upper()
        if any(token in upper for token in ("API_KEY", "TOKEN", "PASSWORD", "SECRET", "COOKIE")):
            redacted.append("<redacted>")
        else:
            redacted.append(part)
    return redacted


def slug(value: str) -> str:
    safe = [ch.lower() if ch.isalnum() else "-" for ch in value]
    return "-".join("".join(safe).split("-")).strip("-") or "job"


def append_jsonl(log_dir: Path, record: Dict[str, Any]) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"{datetime.now().strftime('%Y-%m-%d')}.jsonl"
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


@contextmanager
def job_lock(lock_dir: Path, lock_name: str, enabled: bool = True) -> Iterator[None]:
    if not enabled:
        yield
        return
    lock_dir.mkdir(parents=True, exist_ok=True)
    path = lock_dir / slug(lock_name)
    try:
        path.mkdir()
        (path / "pid").write_text(str(os.getpid()), encoding="utf-8")
        (path / "started_at").write_text(utc_now(), encoding="utf-8")
    except FileExistsError as exc:
        raise JobRunnerError(f"lock already active: {path}") from exc
    try:
        yield
    finally:
        shutil.rmtree(path, ignore_errors=True)


def extract_metrics(text: str) -> Dict[str, Any]:
    for line in reversed((text or "").splitlines()):
        if not line.startswith("ETL_METRICS_JSON="):
            continue
        raw = line.split("=", 1)[1]
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


class InvestmentJobRunner:
    def __init__(
        self,
        manifest: Dict[str, Any],
        *,
        repo_root: Path = REPO_ROOT,
        log_dir: Path = DEFAULT_LOG_DIR,
        lock_dir: Path = DEFAULT_LOCK_DIR,
        variables: Optional[Dict[str, str]] = None,
        dry_run: bool = False,
        no_lock: bool = False,
    ) -> None:
        self.manifest = manifest
        self.repo_root = repo_root
        self.log_dir = log_dir
        self.lock_dir = lock_dir
        self.variables = variables or default_variables(repo_root, {})
        self.dry_run = dry_run
        self.no_lock = no_lock

    def plan_task(self, task_name: str) -> Dict[str, Any]:
        task = self._get_task(task_name)
        command = expand_command(task["command"], self.variables)
        return {
            "task": task_name,
            "description": task.get("description", ""),
            "command": redact_command(command),
            "cwd": expand_value(str(task.get("cwd", "{repo_root}")), self.variables),
            "timeout_seconds": int(task.get("timeout_seconds", 3600)),
        }

    def run_task(self, task_name: str, *, parent_run_id: Optional[str] = None) -> Dict[str, Any]:
        task = self._get_task(task_name)
        plan = self.plan_task(task_name)
        run_id = parent_run_id or uuid.uuid4().hex[:12]
        record_base = {"run_id": run_id, "task": task_name, "parent_run_id": parent_run_id}
        if self.dry_run:
            record = {**record_base, "status": "planned", "planned": plan, "started_at": utc_now(), "finished_at": utc_now()}
            append_jsonl(self.log_dir, record)
            return record

        command = expand_command(task["command"], self.variables)
        cwd = Path(expand_value(str(task.get("cwd", "{repo_root}")), self.variables))
        timeout = int(task.get("timeout_seconds", 3600))
        env = os.environ.copy()
        env.update(expand_env(task.get("env", {}), self.variables))
        stdout_path = self.log_dir / f"{run_id}-{slug(task_name)}.out.log"
        stderr_path = self.log_dir / f"{run_id}-{slug(task_name)}.err.log"

        started = time.time()
        append_jsonl(
            self.log_dir,
            {**record_base, "status": "running", "started_at": utc_now(), "planned": plan},
        )
        try:
            completed = subprocess.run(
                command,
                cwd=str(cwd),
                env=env,
                text=True,
                capture_output=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            stdout_path.write_text(exc.stdout or "", encoding="utf-8")
            stderr_path.write_text(exc.stderr or "", encoding="utf-8")
            record = {
                **record_base,
                "status": "timeout",
                "returncode": -1,
                "duration_seconds": round(time.time() - started, 3),
                "stdout_log": str(stdout_path),
                "stderr_log": str(stderr_path),
                "finished_at": utc_now(),
            }
            append_jsonl(self.log_dir, record)
            return record

        stdout_path.write_text(completed.stdout or "", encoding="utf-8")
        stderr_path.write_text(completed.stderr or "", encoding="utf-8")
        status = "success" if completed.returncode == 0 else "failed"
        record = {
            **record_base,
            "status": status,
            "returncode": completed.returncode,
            "duration_seconds": round(time.time() - started, 3),
            "metrics": extract_metrics(completed.stdout or ""),
            "stdout_log": str(stdout_path),
            "stderr_log": str(stderr_path),
            "finished_at": utc_now(),
        }
        append_jsonl(self.log_dir, record)
        return record

    def run_sequence(self, sequence_name: str, *, skip: Optional[set[str]] = None) -> Dict[str, Any]:
        sequence = self._get_sequence(sequence_name)
        steps = sequence.get("steps", [])
        run_id = uuid.uuid4().hex[:12]
        skipped = skip or set()
        records: List[Dict[str, Any]] = []
        with job_lock(self.lock_dir, sequence_name, enabled=not self.no_lock and not self.dry_run):
            append_jsonl(
                self.log_dir,
                {"run_id": run_id, "sequence": sequence_name, "status": "running", "started_at": utc_now()},
            )
            for step in steps:
                task_name = step.get("task") if isinstance(step, dict) else step
                if task_name in skipped:
                    record = {"run_id": run_id, "task": task_name, "status": "skipped", "finished_at": utc_now()}
                    append_jsonl(self.log_dir, record)
                    records.append(record)
                    continue
                record = self.run_task(task_name, parent_run_id=run_id)
                records.append(record)
                if record.get("status") not in {"success", "planned"}:
                    break
            status = "success" if all(record.get("status") in {"success", "planned", "skipped"} for record in records) else "failed"
            summary = {
                "run_id": run_id,
                "sequence": sequence_name,
                "status": status,
                "tasks": records,
                "finished_at": utc_now(),
            }
            append_jsonl(self.log_dir, summary)
            return summary

    def _get_task(self, task_name: str) -> Dict[str, Any]:
        task = (self.manifest.get("tasks") or {}).get(task_name)
        if not isinstance(task, dict):
            raise JobRunnerError(f"unknown task: {task_name}")
        return task

    def _get_sequence(self, sequence_name: str) -> Dict[str, Any]:
        sequence = (self.manifest.get("sequences") or {}).get(sequence_name)
        if not isinstance(sequence, dict):
            raise JobRunnerError(f"unknown sequence: {sequence_name}")
        return sequence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Investment Hub maintenance jobs from a manifest.")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST), help="Path to job manifest JSON.")
    parser.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR), help="Directory for JSONL and stdout/stderr logs.")
    parser.add_argument("--lock-dir", default=str(DEFAULT_LOCK_DIR), help="Directory for non-overlap lock folders.")
    parser.add_argument("--dry-run", action="store_true", help="Plan jobs without executing commands.")
    parser.add_argument("--no-lock", action="store_true", help="Disable sequence lock, useful for tests only.")
    parser.add_argument("--set", dest="setters", action="append", default=[], help="Template variable override as key=value.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate", help="Validate the manifest.")
    sub.add_parser("list", help="List tasks and sequences.")
    run_task = sub.add_parser("run-task", help="Run one task.")
    run_task.add_argument("task")
    run_task.add_argument("--set", dest="setters_after", action="append", default=[], help="Template variable override as key=value.")
    run_sequence = sub.add_parser("run-sequence", help="Run one sequence.")
    run_sequence.add_argument("sequence")
    run_sequence.add_argument("--skip", action="append", default=[], help="Skip a task name in the sequence.")
    run_sequence.add_argument("--set", dest="setters_after", action="append", default=[], help="Template variable override as key=value.")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        manifest = load_manifest(Path(args.manifest))
        manifest_defaults = {
            str(key): str(value)
            for key, value in (manifest.get("defaults") or {}).items()
        }
        all_setters = list(args.setters or []) + list(getattr(args, "setters_after", []) or [])
        manifest_defaults.update(parse_setters(all_setters))
        variables = default_variables(REPO_ROOT, manifest_defaults)
        runner = InvestmentJobRunner(
            manifest,
            repo_root=REPO_ROOT,
            log_dir=Path(args.log_dir),
            lock_dir=Path(args.lock_dir),
            variables=variables,
            dry_run=args.dry_run,
            no_lock=args.no_lock,
        )
        if args.command == "validate":
            print("manifest ok")
            return 0
        if args.command == "list":
            print(json.dumps({"tasks": sorted(manifest["tasks"]), "sequences": sorted(manifest.get("sequences", {}))}, ensure_ascii=False, indent=2))
            return 0
        if args.command == "run-task":
            print(json.dumps(runner.run_task(args.task), ensure_ascii=False, indent=2))
            return 0
        if args.command == "run-sequence":
            result = runner.run_sequence(args.sequence, skip=set(args.skip or []))
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0 if result.get("status") == "success" else 1
    except JobRunnerError as exc:
        print(f"job runner error: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
