# FoodShield live demo (Windows): starts the dashboard server in its own window (it opens the
# browser on the Streaming tab), then runs the 3-minute demo replay in this window. Every stress
# alert from January 2022 on pops up in the browser as Spark raises it.
#
# Started by RUN_DEMO_LIVE.bat. Needs what RUN_LAPTOP_STEPS.bat set up: Java 17+, the Hadoop
# 3.5.0 files with HADOOP_HOME, and Python 3.12 with the project packages.

# "Continue", as in the other laptop scripts: under "Stop", Windows PowerShell 5.1 treats java's
# version text on stderr as a fatal error.
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$work = "C:\fs6"
function Fail($msg) { Write-Host "STOP: $msg" -ForegroundColor Red; exit 1 }

# Java 17+ (same search as the laptop setup script)
$jdk = $null
$candidates = @()
if ($env:JAVA_HOME) { $candidates += $env:JAVA_HOME }
$candidates += Get-ChildItem "C:\Program Files\Eclipse Adoptium", "C:\Program Files\Java", "C:\Program Files\Microsoft" -Directory -ErrorAction SilentlyContinue |
    Sort-Object Name -Descending | ForEach-Object { $_.FullName }
foreach ($c in $candidates) {
    if ($c -and (Test-Path (Join-Path $c "bin\java.exe"))) {
        $text = (& (Join-Path $c "bin\java.exe") -version 2>&1 | Out-String)
        if ($text -match 'version "(\d+)' -and [int]$Matches[1] -ge 17) { $jdk = $c; break }
    }
}
if (-not $jdk) { Fail "Java 17+ not found. Run RUN_LAPTOP_STEPS.bat first." }
$env:JAVA_HOME = $jdk
$env:PATH = "$jdk\bin;$env:PATH"

# Hadoop native files
$hh = $env:HADOOP_HOME
if (-not $hh) { $hh = [Environment]::GetEnvironmentVariable("HADOOP_HOME", "User") }
if (-not $hh) { $hh = "C:\hadoop" }
if (-not ((Test-Path "$hh\bin\winutils.exe") -and (Test-Path "$hh\bin\hadoop.dll"))) { Fail "Hadoop files not found in $hh\bin. Run RUN_LAPTOP_STEPS.bat first." }
$env:HADOOP_HOME = $hh
$env:PATH = "$hh\bin;$env:PATH"

# Stop Kafka or Spark JVMs left over from an interrupted run of this project (they lock files in C:\fs6).
$kafkaDir = Join-Path $root "Kafka"
$leftover = @(Get-CimInstance Win32_Process -Filter "Name='java.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -and $_.CommandLine.IndexOf($kafkaDir, [System.StringComparison]::OrdinalIgnoreCase) -ge 0 })
foreach ($p in $leftover) { Write-Host "Stopping leftover FoodShield Java process $($p.ProcessId)"; Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue }

New-Item -ItemType Directory -Force -Path $work | Out-Null
Write-Host "1/2 Starting the dashboard server in its own window; the browser opens on the Streaming tab."
Start-Process -FilePath "py" -ArgumentList @("-3.12", "scripts\serve_dashboard.py", "--work-root", $work) -WorkingDirectory $root
Start-Sleep -Seconds 5

Write-Host "2/2 Demo replay: 2016-2021 sent fast to build feature history, then all of 2022 at 1,000 events/s (about 3 minutes)."
Write-Host "    Do not click inside this window while it runs (that pauses it)."
& py -3.12 scripts\run_week6_replay.py --name demo --work-root $work --replay-from 2016-01-01 --measure-from 2022-01-01 --rate 1000
$code = $LASTEXITCODE
Write-Host ""
if ($code -eq 0) { Write-Host "Demo finished; the report is in data\processed\week6_streaming\demo\week6_report.json." -ForegroundColor Green }
else { Write-Host "The demo replay stopped with exit code $code. Fallback: the Streaming tab's 'Replay a saved run' plays the recorded laptop run." -ForegroundColor Yellow }
Write-Host "The dashboard keeps running in its own window; close that window when you are done."
exit $code
