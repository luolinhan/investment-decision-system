param(
    [string]$RepoRoot = "C:\Users\Administrator\research_report_system",
    [string]$PythonExe = "C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe",
    [int]$TranslationLimit = 100,
    [int]$ShortlineLimit = 20,
    [switch]$SkipShortline,
    [switch]$SkipSnapshot
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $RepoRoot)) {
    throw "RepoRoot not found: $RepoRoot"
}

Set-Location $RepoRoot

if (-not (Test-Path "logs")) {
    New-Item -ItemType Directory -Path "logs" | Out-Null
}
if (-not (Test-Path "tmp")) {
    New-Item -ItemType Directory -Path "tmp" | Out-Null
}

if (-not (Test-Path $PythonExe)) {
    $PythonExe = "python"
}

$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:INTELLIGENCE_TRANSLATION_LIMIT = [string]$TranslationLimit
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$logFile = Join-Path $RepoRoot "logs\lead-lag-pretranslate.log"
$lockDir = Join-Path $RepoRoot "tmp\lead-lag-pretranslate.lock"

function Write-TaskLog {
    param([string]$Message)
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $logFile -Value "[$stamp] $Message"
}

function Invoke-Step {
    param(
        [string]$Name,
        [string[]]$ArgsList
    )
    Write-TaskLog "START $Name"
    $stdout = Join-Path $RepoRoot "logs\lead-lag-$Name.out.log"
    $stderr = Join-Path $RepoRoot "logs\lead-lag-$Name.err.log"
    $process = Start-Process `
        -FilePath $PythonExe `
        -WorkingDirectory $RepoRoot `
        -ArgumentList $ArgsList `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -Wait `
        -PassThru `
        -NoNewWindow
    $rc = $process.ExitCode
    if (Test-Path $stdout) {
        Get-Content $stdout -Tail 80 | ForEach-Object { Add-Content -Path $logFile -Value $_ }
    }
    if (Test-Path $stderr) {
        Get-Content $stderr -Tail 80 | ForEach-Object { Add-Content -Path $logFile -Value "[stderr] $_" }
    }
    Write-TaskLog "END $Name rc=$rc"
    if ($rc -ne 0) {
        throw "$Name failed with exit code $rc"
    }
}

$busy = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -ieq "python.exe" -and (
        $_.CommandLine -like "*scripts\sync_intelligence.py*" -or
        $_.CommandLine -like "*scripts\translate_intelligence.py*" -or
        $_.CommandLine -like "*scripts\translate_shortline_events.py*" -or
        $_.CommandLine -like "*scripts\lead_lag_aliyun_collector.py*"
    )
}

if ($busy.Count -gt 0) {
    Write-TaskLog "SKIP active collector or translator process count=$($busy.Count)"
    exit 0
}

try {
    New-Item -ItemType Directory -Path $lockDir -ErrorAction Stop | Out-Null
} catch {
    Write-TaskLog "SKIP lock active: $lockDir"
    exit 0
}

try {
    Write-TaskLog "Lead-Lag pre-collection and pre-translation task started"
    Invoke-Step -Name "collect_intelligence" -ArgsList @("scripts\sync_intelligence.py")
    Invoke-Step -Name "translate_intelligence" -ArgsList @("scripts\translate_intelligence.py")

    if (-not $SkipShortline) {
        Invoke-Step -Name "translate_shortline_t0" -ArgsList @("scripts\translate_shortline_events.py", "--limit", [string]$ShortlineLimit)
    }

    if (-not $SkipSnapshot) {
        Invoke-Step -Name "export_lead_lag_snapshot" -ArgsList @(
            "scripts\lead_lag_aliyun_collector.py",
            "--output",
            "data\lead_lag\collector_snapshot.json"
        )
    }

    Write-TaskLog "Lead-Lag pre-collection and pre-translation task completed"
} finally {
    Remove-Item -Path $lockDir -Recurse -Force -ErrorAction SilentlyContinue
}
