@echo off
cd /d %~dp0\..
powershell -ExecutionPolicy Bypass -File scripts\start_lead_lag_demo.ps1 %*
