[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("overnight_digest", "pre_open_playbook", "morning_review", "close_review", "us_watch_mapping")]
    [string]$Slot,

    [string]$RepoRoot = "C:\Users\Administrator\research_report_system",
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"

function Write-RunnerLog {
    param(
        [string]$LogFile,
        [string]$Message
    )
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "[$stamp] $Message" | Out-File -FilePath $LogFile -Encoding utf8 -Append
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

$briefScript = Join-Path $RepoRoot "scripts\export_lead_lag_brief.py"
if (-not (Test-Path -LiteralPath $briefScript)) {
    throw "Lead-Lag brief export script not found: $briefScript"
}

$logsDir = Join-Path $RepoRoot "logs\lead_lag_briefs"
if (-not (Test-Path -LiteralPath $logsDir)) {
    New-Item -Path $logsDir -ItemType Directory -Force | Out-Null
}

$knowledgeBaseDirName = -join ([char]0x77E5, [char]0x8BC6, [char]0x5E93)
$adminVault = Join-Path "C:\Users\Administrator\Documents\Obsidian" $knowledgeBaseDirName
if (-not $env:INVESTMENT_OBSIDIAN_VAULT -and (Test-Path -LiteralPath $adminVault)) {
    $env:INVESTMENT_OBSIDIAN_VAULT = $adminVault
}

$logFile = Join-Path $logsDir "LeadLagBrief-$Slot.log"
$resolvedPython = Resolve-PythonExe -Candidate $PythonExe -Root $RepoRoot
Write-RunnerLog -LogFile $logFile -Message "Starting Lead-Lag brief export: slot=$Slot repo=$RepoRoot python=$resolvedPython vault=$env:INVESTMENT_OBSIDIAN_VAULT"

Push-Location $RepoRoot
try {
    & $resolvedPython $briefScript --slot $Slot --markdown --json --obsidian *>> $logFile
    $exitCode = $LASTEXITCODE
    Write-RunnerLog -LogFile $logFile -Message "Finished Lead-Lag brief export: slot=$Slot exit=$exitCode"
    exit $exitCode
}
catch {
    Write-RunnerLog -LogFile $logFile -Message "Failed Lead-Lag brief export: slot=$Slot error=$($_.Exception.Message)"
    throw
}
finally {
    Pop-Location
}
