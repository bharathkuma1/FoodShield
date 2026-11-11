# Week 6 laptop run (decision D12): checks prerequisites, then runs the four replay runs.
#
# From the repository folder in PowerShell:
#   powershell -ExecutionPolicy Bypass -File scripts\week6_laptop.ps1
#
# Optional settings (set before running if they differ on your machine):
#   $env:JAVA_HOME    folder of a Java 17+ JDK  (e.g. C:\Program Files\Eclipse Adoptium\jdk-21.0.x-hotspot)
#   $env:HADOOP_HOME  folder whose bin\ holds winutils.exe and hadoop.dll for Hadoop 3.5.0
#   $env:FS6_WORK     short folder without spaces for Kafka data and Spark checkpoints (default C:\fs6)
#   $env:FS6_PYTHON   Python launcher arguments (default "py -3.12")

# Native tools (java, python, Spark) write normal progress to stderr; rely on explicit checks below.
$ErrorActionPreference = "Continue"
Set-Location (Split-Path -Parent $PSScriptRoot)
$work = if ($env:FS6_WORK) { $env:FS6_WORK } else { "C:\fs6" }
# Keep the launcher arguments an array: splatting a single string passes it one character at a time.
$pyLine = if ($env:FS6_PYTHON) { $env:FS6_PYTHON } else { "py -3.12" }
$pyParts = @($pyLine.Split(" ", [System.StringSplitOptions]::RemoveEmptyEntries))
$pyExe = $pyParts[0]
$pyArgs = @($pyParts | Select-Object -Skip 1)

function Fail($msg) { Write-Host "STOP: $msg" -ForegroundColor Red; exit 1 }

Write-Host "1/6 Checking prerequisites"
if (-not $env:JAVA_HOME) {
    $jdk = Get-ChildItem "C:\Program Files\Eclipse Adoptium", "C:\Program Files\Java", "C:\Program Files\Microsoft" -Directory -ErrorAction SilentlyContinue |
        Where-Object { Test-Path (Join-Path $_.FullName "bin\java.exe") } | Sort-Object Name -Descending | Select-Object -First 1
    if ($jdk) { $env:JAVA_HOME = $jdk.FullName }
}
if (-not $env:JAVA_HOME -or -not (Test-Path "$env:JAVA_HOME\bin\java.exe")) { Fail "Java 17+ not found. Install a JDK (see docs\WEEK6_STREAMING.md) or set JAVA_HOME." }
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
if (-not $env:HADOOP_HOME -or -not (Test-Path "$env:HADOOP_HOME\bin\winutils.exe") -or -not (Test-Path "$env:HADOOP_HOME\bin\hadoop.dll")) {
    Fail "HADOOP_HOME must point to Hadoop 3.5.0 native files (bin\winutils.exe and bin\hadoop.dll)."
}
foreach ($f in "Kafka\kafka_2.13-4.4.0.tgz", "Kafka\spark-sql-kafka-0-10_2.13-4.3.0.jar", "Kafka\spark-token-provider-kafka-0-10_2.13-4.3.0.jar",
               "Kafka\kafka-clients-3.9.2.jar", "Kafka\commons-pool2-2.13.1.jar", "data\raw\nasa_power_grid\nasa_power_grid_daily_2001_2022.csv") {
    if (-not (Test-Path $f)) { Fail "Missing $f" }
}
$pyVersion = (& $pyExe @pyArgs --version 2>&1 | Out-String).Trim()
Write-Host $pyVersion
if ($pyVersion -notmatch 'Python 3\.(1[1-9])') { Fail "Python 3.11+ not found with '$pyLine' (got: $pyVersion). Set FS6_PYTHON." }
& "$env:JAVA_HOME\bin\java.exe" -version

# Stop Kafka brokers or Spark JVMs left over from an earlier, interrupted run of this project:
# on Windows they keep files in the work folder locked. Only processes whose command line names this
# project's Kafka folder are stopped.
$kafkaDir = (Join-Path (Get-Location) "Kafka")
$leftover = @(Get-CimInstance Win32_Process -Filter "Name='java.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -and $_.CommandLine.IndexOf($kafkaDir, [System.StringComparison]::OrdinalIgnoreCase) -ge 0 })
foreach ($p in $leftover) { Write-Host "Stopping leftover FoodShield Java process $($p.ProcessId)"; Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue }

Write-Host "2/6 Installing Python packages (pyspark 4.3.0, kafka-python 3.0.13)"
& $pyExe @pyArgs -m pip install --quiet -r requirements.txt
if ($LASTEXITCODE -ne 0) { Fail "pip install failed" }

$runs = @(
    @("--name", "laptop_full_max", "--duplicate-fraction", "0.01"),
    @("--name", "laptop_paced_100", "--replay-from", "2011-01-01", "--measure-from", "2022-01-01", "--rate", "100"),
    @("--name", "laptop_paced_250", "--replay-from", "2011-01-01", "--measure-from", "2022-01-01", "--rate", "250"),
    @("--name", "laptop_restart", "--kill-consumer-after-batches", "60")
)
$i = 3
foreach ($run in $runs) {
    Write-Host "$i/6 Run $($run[1])"
    & $pyExe @pyArgs scripts\run_week6_replay.py @run --work-root $work
    if ($LASTEXITCODE -ne 0) { Write-Host "Run $($run[1]) reported a failed check; see data\processed\week6_streaming\$($run[1])\week6_report.json" -ForegroundColor Yellow }
    $i++
}
Write-Host "Done. Reports: data\processed\week6_streaming\laptop_*\week6_report.json"
