# FoodShield live demo (Windows): starts the dashboard server in its own window (it opens the
# browser on the Streaming tab), then runs the 3-minute demo replay in this window. Every stress
# alert from January 2022 on pops up in the browser as Spark raises it.
#
# Started by RUN_DEMO_LIVE.bat. Needs Java 17+, the Hadoop 3.5.0 native files (HADOOP_HOME), Python
# 3.11+ with the project packages, and the Kafka folder. Python, Java, Hadoop and the work folder are
# found by scripts\foodshield_env.ps1; see the settings listed at the top of that file.

# "Continue", as in the other laptop scripts: under "Stop", Windows PowerShell 5.1 treats java's
# version text on stderr as a fatal error.
$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "foodshield_env.ps1")
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
function Fail($msg) { Write-Host "STOP: $msg" -ForegroundColor Red; exit 1 }

try { $fs = Initialize-FsEnvironment } catch { Fail $_.Exception.Message }
$py = $fs.Python
$work = $fs.Work
Write-Host "Python: $($py.Version) ($($py.Source))   Java: $($fs.Java)   Hadoop: $($fs.Hadoop)   Work folder: $work" -ForegroundColor DarkGray

Stop-FsLeftoverJava
New-Item -ItemType Directory -Force -Path $work | Out-Null

Write-Host "1/2 Starting the dashboard server in its own window; the browser opens on the Streaming tab."
Start-Process -FilePath $py.Exe -ArgumentList ($py.Args + @("scripts\serve_dashboard.py", "--work-root", $work)) -WorkingDirectory $root
Start-Sleep -Seconds 5

Write-Host "2/2 Demo replay: 2016-2021 sent fast to build feature history, then all of 2022 at 1,000 events/s (about 3 minutes)."
Write-Host "    Do not click inside this window while it runs (that pauses it)."
$pyArgs = $py.Args
& $py.Exe @pyArgs scripts\run_week6_replay.py --name demo --work-root $work --replay-from 2016-01-01 --measure-from 2022-01-01 --rate 1000
$code = $LASTEXITCODE
Write-Host ""
if ($code -eq 0) { Write-Host "Demo finished; the report is in data\processed\week6_streaming\demo\week6_report.json." -ForegroundColor Green }
else { Write-Host "The demo replay stopped with exit code $code. Fallback: the Streaming tab's 'Replay a saved run' plays the recorded laptop run." -ForegroundColor Yellow }
Write-Host "The dashboard keeps running in its own window; close that window when you are done."
exit $code
