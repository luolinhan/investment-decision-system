@echo off
cd /d %~dp0\..
powershell -ExecutionPolicy Bypass -File scripts\sync_lead_lag_aliyun_snapshot.ps1 %*
