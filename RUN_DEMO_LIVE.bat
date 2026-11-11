@echo off
rem FoodShield live demo: dashboard server in its own window (opens the browser), then the 3-minute replay here.
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\demo_live.ps1"
echo.
pause
