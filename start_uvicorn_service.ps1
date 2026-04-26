$repoRoot = "C:\Users\Administrator\research_report_system"
$pythonExe = "C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe"
$serviceScript = Join-Path $repoRoot "run_uvicorn_service.py"
$taskName = "InvestmentHub8080"

New-Item -ItemType Directory -Force -Path (Join-Path $repoRoot "logs") | Out-Null

$env:PYTHONPATH = $repoRoot
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUNBUFFERED = "1"
$env:APP_ENV = "production"
$env:TZ = "Asia/Shanghai"
$env:RADAR_SNAPSHOT_ONLY = "1"

if (-not (Test-Path $pythonExe)) {
    $pythonExe = "python"
}
if (-not (Test-Path $serviceScript)) {
    throw "Service script not found: $serviceScript"
}

$existing = @(Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue)[0]
if ($null -ne $existing) {
    Stop-Process -Id $existing.OwningProcess -Force
    Start-Sleep -Seconds 1
}

$taskCommand = "`"$pythonExe`" `"$serviceScript`""
& schtasks.exe /Create /TN $taskName /SC ONSTART /TR $taskCommand /RU SYSTEM /F | Out-Null
& schtasks.exe /Run /TN $taskName | Out-Null

Start-Sleep -Seconds 4

if ($null -eq @(Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue)[0]) {
    throw "Investment Hub did not open port 8080 after running task $taskName."
}
