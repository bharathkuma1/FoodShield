@echo off
rem Runs every FoodShield laptop step (scripts\laptop_all_steps.ps1). Safe to run again after a stop.
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\laptop_all_steps.ps1"
echo.
pause
