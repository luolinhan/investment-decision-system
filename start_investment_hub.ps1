$repoRoot = "C:\Users\Administrator\research_report_system"
$serviceScript = Join-Path $repoRoot "run_uvicorn_service.py"
$serviceWrapper = Join-Path $repoRoot "start_uvicorn_service.bat"
$logDir = Join-Path $repoRoot "logs"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$existing = @(Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue)[0]
if ($null -ne $existing) {
    Stop-Process -Id $existing.OwningProcess -Force
    Start-Sleep -Seconds 1
}

$env:PYTHONPATH = $repoRoot
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUNBUFFERED = "1"
$env:APP_ENV = "production"
$env:TZ = "Asia/Shanghai"
$env:RADAR_SNAPSHOT_ONLY = "1"

if (-not (Test-Path $serviceScript)) {
    throw "Detached service script not found: $serviceScript"
}
if (-not (Test-Path $serviceWrapper)) {
    throw "Detached service wrapper not found: $serviceWrapper"
}

& $serviceWrapper

Start-Sleep -Seconds 2

if ($null -eq @(Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue)[0]) {
    throw "Investment Hub did not open port 8080. Check $logDir\\uvicorn_service.log."
}
