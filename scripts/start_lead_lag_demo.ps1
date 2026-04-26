[CmdletBinding()]
param(
    [string]$Host = "0.0.0.0",
    [int]$Port = 8080
)

$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:LEAD_LAG_USE_SAMPLE_DATA = "1"

Write-Host "[start_lead_lag_demo.ps1] Starting Lead-Lag demo on http://$Host`:$Port/investment/lead-lag"
python -m uvicorn app.main:app --host $Host --port $Port
