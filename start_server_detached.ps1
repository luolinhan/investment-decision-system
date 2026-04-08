$ErrorActionPreference = "Stop"

$Root = "C:\Users\Administrator\research_report_system"
$PythonPath = "C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe"
$LogDir = Join-Path $Root "logs"

if (-not (Test-Path $Root)) {
    throw "Project directory not found: $Root"
}
if (-not (Test-Path $PythonPath)) {
    throw "Python not found: $PythonPath"
}
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

$listenPids = Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique
foreach ($procId in $listenPids) {
    Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
}

$stdout = Join-Path $LogDir "server.log"
$stderr = Join-Path $LogDir "server.err.log"

Start-Process -FilePath $PythonPath `
    -WorkingDirectory $Root `
    -WindowStyle Hidden `
    -ArgumentList @(
        "-m", "uvicorn",
        "app.main:app",
        "--host", "0.0.0.0",
        "--port", "8080",
        "--workers", "1",
        "--no-access-log"
    ) `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr

Start-Sleep -Seconds 2

$newPid = Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue |
    Select-Object -First 1 -ExpandProperty OwningProcess

if (-not $newPid) {
    throw "uvicorn failed to listen on 8080"
}

Write-Output "started_pid=$newPid"
