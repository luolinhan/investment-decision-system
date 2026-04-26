[CmdletBinding()]
param(
    [string]$Output = "data/lead_lag/collector_snapshot.json"
)

$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:LEAD_LAG_COLLECTOR_MODE = "aliyun_snapshot"

Write-Host "[sync_lead_lag_aliyun_snapshot.ps1] Exporting Lead-Lag collector snapshot to $Output"
python scripts/lead_lag_aliyun_collector.py --output $Output --pretty
