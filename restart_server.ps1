Set-Location 'C:\Users\Administrator\research_report_system'
Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1 | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
New-Item -ItemType Directory -Force -Path logs | Out-Null
$taskName = 'InvestmentHubUvicorn'
$action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument '/c cd /d C:\Users\Administrator\research_report_system && python -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 1 --no-access-log > logs\server.log 2> logs\server.err.log'
$trigger = New-ScheduledTaskTrigger -Once -At ((Get-Date).AddMinutes(1))
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -RunLevel Highest -Force | Out-Null
Start-ScheduledTask -TaskName $taskName
Write-Output 'scheduled'
