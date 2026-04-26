[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "Medium")]
param(
    [string]$RepoRoot = "C:\Users\Administrator\research_report_system",
    [string]$PythonExe = "python",
    [switch]$Force
)

$ErrorActionPreference = "Stop"

function Write-TaskLog {
    param([string]$Message)
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$stamp] $Message"
}

function Resolve-PythonExe {
    param(
        [string]$Candidate,
        [string]$Root
    )
    if ($Candidate -and (Test-Path -LiteralPath $Candidate)) {
        return $Candidate
    }
    $repoVenv = Join-Path $Root "venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $repoVenv) {
        return $repoVenv
    }
    $repoDotVenv = Join-Path $Root ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $repoDotVenv) {
        return $repoDotVenv
    }
    $command = Get-Command $Candidate -ErrorAction SilentlyContinue
    if ($command -and $command.Source) {
        return $command.Source
    }
    return $Candidate
}

if (-not (Test-Path -LiteralPath $RepoRoot)) {
    throw "RepoRoot not found: $RepoRoot"
}

$scriptPath = Join-Path $RepoRoot "scripts\export_lead_lag_brief.py"
if (-not (Test-Path -LiteralPath $scriptPath)) {
    throw "Lead-Lag brief export script not found: $scriptPath"
}

$runnerPath = Join-Path $RepoRoot "scripts\run_lead_lag_brief_task.ps1"
if (-not (Test-Path -LiteralPath $runnerPath)) {
    throw "Lead-Lag brief task runner not found: $runnerPath"
}

$ResolvedPythonExe = Resolve-PythonExe -Candidate $PythonExe -Root $RepoRoot
Write-TaskLog "Using Python: $ResolvedPythonExe"

$logsDir = Join-Path $RepoRoot "logs\lead_lag_briefs"
if (-not (Test-Path -LiteralPath $logsDir)) {
    if ($PSCmdlet.ShouldProcess($logsDir, "Create log directory")) {
        New-Item -Path $logsDir -ItemType Directory -Force | Out-Null
        Write-TaskLog "Created logs directory: $logsDir"
    }
}

$taskSpecs = @(
    @{ Name = "LeadLagBrief0600"; Time = "06:00"; Slot = "overnight_digest" },
    @{ Name = "LeadLagBrief0820"; Time = "08:20"; Slot = "pre_open_playbook" },
    @{ Name = "LeadLagBrief1140"; Time = "11:40"; Slot = "morning_review" },
    @{ Name = "LeadLagBrief1515"; Time = "15:15"; Slot = "close_review" },
    @{ Name = "LeadLagBrief2130"; Time = "21:30"; Slot = "us_watch_mapping" }
)

foreach ($spec in $taskSpecs) {
    $taskName = $spec.Name
    $slot = $spec.Slot
    $time = $spec.Time
    $cmdPath = Join-Path $RepoRoot "scripts\$taskName.cmd"
    $cmdContent = @(
        "@echo off",
        "powershell -NoProfile -ExecutionPolicy Bypass -File ""%~dp0run_lead_lag_brief_task.ps1"" -RepoRoot ""$RepoRoot"" -PythonExe ""$ResolvedPythonExe"" -Slot $slot",
        "exit /b %ERRORLEVEL%"
    )
    if ($PSCmdlet.ShouldProcess($cmdPath, "Create brief task wrapper")) {
        $cmdContent | Set-Content -LiteralPath $cmdPath -Encoding ascii
        Write-TaskLog "Wrote wrapper: $cmdPath"
    }
    $taskRun = $cmdPath

    if ($Force -and $PSCmdlet.ShouldProcess($taskName, "Delete existing scheduled task before recreate")) {
        schtasks.exe /Delete /TN $taskName /F | Out-Null
        Write-TaskLog "Deleted existing task: $taskName"
    }

    if ($PSCmdlet.ShouldProcess($taskName, "Create or update scheduled task at $time")) {
        $createArgs = @(
            "/Create",
            "/TN", $taskName,
            "/TR", $taskRun,
            "/SC", "DAILY",
            "/ST", $time,
            "/RU", "SYSTEM",
            "/RL", "HIGHEST",
            "/F"
        )
        schtasks.exe @createArgs | Out-Null
        Write-TaskLog "Upserted task: $taskName time=$time slot=$slot"
    }
}

Write-TaskLog "Lead-Lag brief scheduled tasks setup finished."
