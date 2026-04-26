# Investment Hub 运维维护入口

更新日期：2026-04-26

本页描述不改动数据库结构的维护方案：统一外部脚本入口、统一百炼调用、统一日志和锁，降低后续 Lead-Lag、情报雷达、短线映射继续变复杂后的维护成本。

## 当前边界

- 数据库继续使用现有 SQLite 文件，不做迁移、不新增强制表结构。
- 现有 Windows 计划任务和 bat/ps1 入口保持兼容。
- 新增的任务编排器只负责调度已有脚本、记录运行结果、避免同一序列重叠执行。
- 百炼 API Key 仍只从 `.env.local`、`.env` 或 Windows 任务环境变量读取，仓库和日志不记录密钥正文。

## 统一任务编排器

入口：

```bash
python scripts/investment_job_runner.py validate
python scripts/investment_job_runner.py list
python scripts/investment_job_runner.py --dry-run run-sequence lead_lag_pretranslate
```

常用参数：

- `--manifest`: 指定任务清单，默认 `config/job_manifest.example.json`
- `--dry-run`: 只输出计划，不执行脚本
- `--set key=value`: 覆盖清单变量，例如 `--set translation_limit=300`
- `--skip task_name`: 在运行序列时跳过某个任务
- `--log-dir`: JSONL 与 stdout/stderr 日志目录，默认 `logs/job-runs`
- `--lock-dir`: 序列互斥锁目录，默认 `tmp/job-locks`

示例：

```bash
python scripts/investment_job_runner.py --dry-run run-sequence lead_lag_pretranslate --set translation_limit=300
python scripts/investment_job_runner.py run-sequence lead_lag_pretranslate --skip translate_shortline_t0
python scripts/investment_job_runner.py run-task translate_intelligence --set translation_limit=50
```

## 任务清单约定

清单文件使用 JSON，核心字段：

- `tasks`: 任务定义，每个任务包含 `command`、`cwd`、`timeout_seconds`、可选 `env`
- `sequences`: 多任务序列，只引用 `tasks` 中存在的任务
- `defaults`: 可被 `{variable}` 引用的默认变量

命令中可使用变量：

- `{python}`: 当前 Python 解释器
- `{repo_root}`: 仓库根目录
- `{date}`: 当前日期
- 清单 `defaults` 和 CLI `--set` 传入的变量

脚本如需把关键指标交给编排器，应在 stdout 输出一行：

```text
ETL_METRICS_JSON={"records_processed":10,"records_failed":0}
```

编排器会把该行写入 JSONL 运行记录的 `metrics` 字段。

## 百炼调用约定

统一入口：`app/services/bailian_client.py`

已接入：

- `scripts/translate_intelligence.py`
- `app/services/shortline_service.py`

环境变量：

- `BAILIAN_API_KEY` 或 `DASHSCOPE_API_KEY`
- `BAILIAN_BASE_URL` 或 `DASHSCOPE_BASE_URL`
- `BAILIAN_MODEL` 或 `DASHSCOPE_MODEL`
- `BAILIAN_TIMEOUT_SECONDS`

调用规则：

- 未配置 API Key 时返回空结果，由业务脚本走原有 fallback。
- JSON 输出解析失败时，会自动发起一次 JSON 修复请求。
- 日志、异常和清单中不得写入 API Key、token、cookie、数据库密码。

## Windows 验证命令

生产 Windows 当前仍以现有服务和计划任务为主。阶段性发布前建议至少执行：

```powershell
python -m py_compile app\services\bailian_client.py app\services\shortline_service.py scripts\translate_intelligence.py scripts\investment_job_runner.py
python scripts\investment_job_runner.py validate
python scripts\investment_job_runner.py --dry-run run-sequence lead_lag_pretranslate --skip translate_shortline_t0 --set translation_limit=300
```

如需把某个计划任务逐步迁移到统一编排器，先用 `--dry-run` 对齐命令，再把 Windows Task Scheduler 的入口替换为对应 `run-task` 或 `run-sequence` 命令。
