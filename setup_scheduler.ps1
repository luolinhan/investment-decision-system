# Windows定时任务设置脚本
# 以管理员身份运行PowerShell执行此脚本

$TaskName = "InvestmentDataUpdate"
$TaskDescription = "每日更新投资决策系统股票数据"
$ScriptPath = "C:\Users\Administrator\research_report_system\daily_update.bat"

# 删除已存在的任务
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

# 创建触发器：每天下午3:30执行（股市收盘后）
$Trigger = New-ScheduledTaskTrigger -Daily -At "15:30"

# 创建动作
$Action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c $ScriptPath" -WorkingDirectory "C:\Users\Administrator\research_report_system"

# 创建任务设置
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd -AllowStartIfOnBatteries

# 注册任务
Register-ScheduledTask -TaskName $TaskName -Description $TaskDescription -Trigger $Trigger -Action $Action -Settings $Settings -User "Administrator" -RunLevel Highest

Write-Host "定时任务已创建: $TaskName"
Write-Host "执行时间: 每天 15:30"
Write-Host "执行脚本: $ScriptPath"