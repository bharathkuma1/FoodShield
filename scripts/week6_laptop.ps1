# Week 6 laptop run (decision D12): checks prerequisites, then runs the four replay runs.
#
# From the repository folder in PowerShell:
#   powershell -ExecutionPolicy Bypass -File scripts\week6_laptop.ps1
#
# Python, Java, Hadoop and the work folder are found by scripts\foodshield_env.ps1; the settings
# that override discovery (FOODSHIELD_PYTHON, JAVA_HOME, HADOOP_HOME, FOODSHIELD_WORK_ROOT) are listed there.

# Native tools (java, python, Spark) write normal progress to stderr; rely on explicit checks below.
$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "foodshield_env.ps1")
Set-Location (Split-Path -Parent $PSScriptRoot)

function Fail($msg) { Write-Host "STOP: $msg" -ForegroundColor Red; exit 1 }

Write-Host "1/6 Checking prerequisites"
try { $fs = Initialize-FsEnvironment } catch { Fail $_.Exception.Message }
$work = $fs.Work
# Keep the launcher arguments an array: splatting a single string passes it one character at a time.
$pyExe = $fs.Python.Exe
$pyArgs = @($fs.Python.Args)
Write-Host "Python: $($fs.Python.Version) ($($fs.Python.Source))   Hadoop: $($fs.Hadoop)   Work folder: $work"
foreach ($f in "Kafka\kafka_2.13-4.4.0.tgz", "Kafka\spark-sql-kafka-0-10_2.13-4.3.0.jar", "Kafka\spark-token-provider-kafka-0-10_2.13-4.3.0.jar",
               "Kafka\kafka-clients-3.9.2.jar", "Kafka\commons-pool2-2.13.1.jar", "data\raw\nasa_power_grid\nasa_power_grid_daily_2001_2022.csv") {
    if (-not (Test-Path $f)) { Fail "Missing $f" }
}
& "$env:JAVA_HOME\bin\java.exe" -version

# Stop Kafka brokers or Spark JVMs left over from an earlier, interrupted run (they lock files in the work folder).
Stop-FsLeftoverJava

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
