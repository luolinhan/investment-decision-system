# Windows scheduler setup using schtasks
# Run as Administrator:
#   PowerShell -ExecutionPolicy Bypass -File .\setup_scheduler_v2.ps1

$Root = "C:\Users\Administrator\research_report_system"
$MinuteTaskName = "InvestmentMinuteRuntimeRefresh"
$PreopenTaskName = "InvestmentPreopenStrategy"
$BriefPreopenTaskName = "InvestmentLLMBriefPreopen"
$BriefPostcloseTaskName = "InvestmentLLMBriefPostclose"

$MinuteCommand = "cmd /c `"$Root\minute_runtime_refresh_task.bat`""
$PreopenCommand = "cmd /c `"$Root\generate_preopen_strategy_task.bat`""
$BriefCommand = "cmd /c `"$Root\generate_llm_daily_brief_task.bat`""

cmd /c "schtasks /Delete /TN `"$MinuteTaskName`" /F" | Out-Null
cmd /c "schtasks /Delete /TN `"$PreopenTaskName`" /F" | Out-Null
cmd /c "schtasks /Delete /TN `"$BriefPreopenTaskName`" /F" | Out-Null
cmd /c "schtasks /Delete /TN `"$BriefPostcloseTaskName`" /F" | Out-Null

cmd /c "schtasks /Create /TN `"$MinuteTaskName`" /TR `"$MinuteCommand`" /SC MINUTE /MO 1 /RU Administrator /RL HIGHEST /F"
cmd /c "schtasks /Create /TN `"$PreopenTaskName`" /TR `"$PreopenCommand`" /SC DAILY /ST 08:50 /RU Administrator /RL HIGHEST /F"
cmd /c "schtasks /Create /TN `"$BriefPreopenTaskName`" /TR `"$BriefCommand`" /SC DAILY /ST 08:35 /RU Administrator /RL HIGHEST /F"
cmd /c "schtasks /Create /TN `"$BriefPostcloseTaskName`" /TR `"$BriefCommand`" /SC DAILY /ST 18:10 /RU Administrator /RL HIGHEST /F"

Write-Host "Created tasks:"
Write-Host "  1) $MinuteTaskName - every 1 minute"
Write-Host "  2) $PreopenTaskName - daily 08:50"
Write-Host "  3) $BriefPreopenTaskName - daily 08:35"
Write-Host "  4) $BriefPostcloseTaskName - daily 18:10"
