$repoRoot = "C:\Users\Administrator\research_report_system"
$pythonExe = "C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe"
$logDir = Join-Path $repoRoot "logs"
$stdoutLog = Join-Path $logDir "investment_hub.out.log"
$stderrLog = Join-Path $logDir "investment_hub.err.log"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$existing = @(Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue)[0]
if ($null -ne $existing) {
    Stop-Process -Id $existing.OwningProcess -Force
    Start-Sleep -Seconds 1
}

$arguments = @(
    "-m",
    "uvicorn",
    "app.main:app",
    "--host",
    "0.0.0.0",
    "--port",
    "8080",
    "--no-access-log"
)

$env:RADAR_SNAPSHOT_ONLY = "1"

Start-Process `
    -FilePath $pythonExe `
    -WorkingDirectory $repoRoot `
    -ArgumentList $arguments `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog `
    -WindowStyle Hidden
