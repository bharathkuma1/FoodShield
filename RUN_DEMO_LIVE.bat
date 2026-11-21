@echo off
rem FoodShield live demo: dashboard server in its own window (opens the browser), then the 3-minute replay here.
rem Optional: another stress definition for the models, e.g. RUN_DEMO_LIVE.bat 5 6 (5 percent within 6 months).
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\demo_live.ps1" %*
echo.
pause
