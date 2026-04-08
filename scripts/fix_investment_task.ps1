$taskName = 'InvestmentHubUvicorn'
$projectDir = 'C:\Users\Administrator\research_report_system'

$action = New-ScheduledTaskAction `
    -Execute 'cmd.exe' `
    -Argument '/c C:\Users\Administrator\research_report_system\start_investment_hub.bat' `
    -WorkingDirectory $projectDir

$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -RunLevel Highest `
    -Force | Out-Null

Write-Output 'task-updated'
