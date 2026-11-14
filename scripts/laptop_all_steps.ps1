# FoodShield: every laptop step in one go (Week 8).
#
# Start it by double-clicking RUN_LAPTOP_STEPS.bat in the project folder, or from PowerShell:
#   powershell -ExecutionPolicy Bypass -File scripts\laptop_all_steps.ps1
#
# Steps (each is skipped when it is already done, so the script can simply be run again after a stop):
#   1 tidy the downloads out of the project folder (the duplicate archive goes to the Recycle Bin)
#   2 make the folder a Git repository from foodshield_v2.bundle
#   3 Java 21 (installed with winget if no JDK 17+ is found; winget asks you to accept its terms)
#   4 Hadoop 3.5.0 native files in HADOOP_HOME\bin (if HADOOP_HOME is not set yet: hadoop\bin on the
#     system drive; from Bharath; the script waits for them), and HADOOP_HOME saved for future windows
#   5 Python packages and the test suite (Python found as in scripts\foodshield_env.ps1)
#   6 the four Week 6 streaming runs (scripts\week6_laptop.ps1, about 45-60 minutes)
#   7 laptop results into docs\WEEK6_LAPTOP_RESULTS.md and the dashboard
#   8 demo rehearsal: the slide-11 command, timed
#   9 batch scaling series on the laptop (optional: only when logs\laptop_steps\RUN_BATCH_SERIES exists;
#     about 30 minutes; needs about 12 GB free on the work folder drive)
#
# Logs: logs\laptop_steps\ (status.txt plus one log per command).

$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "foodshield_env.ps1")
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$logDir = Join-Path $root "logs\laptop_steps"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$status = Join-Path $logDir "status.txt"
$env:PYTHONUNBUFFERED = "1"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"
$work = Get-FsWorkRoot

function Say($msg, $color = "Cyan") {
    $line = "{0:yyyy-MM-dd HH:mm:ss}  {1}" -f (Get-Date), $msg
    Write-Host $line -ForegroundColor $color
    Add-Content -Path $status -Value $line -Encoding UTF8
}

function Run($name, $command) {
    # cmd merges stdout and stderr; the output is shown and kept in logs\laptop_steps\<name>.log.
    Say "  > $command" "DarkGray"
    $log = Join-Path $logDir "$name.log"
    cmd /c "$command 2>&1" | Tee-Object -FilePath $log | Out-Host
    $code = $LASTEXITCODE
    Say "  exit code $code ($name)" "DarkGray"
    return $code
}

# Keep the laptop awake while this window runs (plugged in, lid open).
Add-Type -Namespace FoodShield -Name Power -MemberDefinition '[System.Runtime.InteropServices.DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint esFlags);'
[void][FoodShield.Power]::SetThreadExecutionState([uint32]2147483649)

$problems = New-Object System.Collections.ArrayList
function Finish($code) {
    [void][FoodShield.Power]::SetThreadExecutionState([uint32]2147483648)
    if ($code -eq 0 -and $problems.Count -eq 0) { Say "ALL DONE. Tell Claude the laptop steps have finished." "Green" }
    elseif ($code -eq 0) { Say ("FINISHED WITH PROBLEMS: " + ($problems -join "; ") + ". Tell Claude.") "Yellow" }
    exit $code
}

Say "FoodShield laptop steps started in $root" "Green"

# ---------------------------------------------------------------- 1
Say "Step 1/9: tidy the downloads"
$moves = @(
    @("Prices_E_All_Data_(Normalized).zip", "data\raw\faostat_bulk"),
    @("Production_Crops_Livestock_E_All_Data_(Normalized).zip", "data\raw\faostat_bulk"),
    @("Trade_CropsLivestock_E_All_Data_(Normalized).zip", "data\raw\faostat_bulk"),
    @("Production_Crops_Livestock_E_All_Data_(Normalized)_1.zip", "_to_delete")
)
foreach ($m in $moves) {
    $src = Join-Path $root $m[0]
    $dstDir = Join-Path $root $m[1]
    if (Test-Path -LiteralPath $src) {
        New-Item -ItemType Directory -Force -Path $dstDir | Out-Null
        if (Test-Path -LiteralPath (Join-Path $dstDir $m[0])) { Say "  $($m[0]) is already in $($m[1]); the copy in the project folder stays" "Yellow" }
        else { Move-Item -LiteralPath $src -Destination $dstDir; Say "  moved $($m[0]) to $($m[1])" }
    }
}
$parts = Join-Path $root "data\raw\nasa_power_grid\download_parts"
New-Item -ItemType Directory -Force -Path $parts | Out-Null
Get-ChildItem -LiteralPath $root -Filter "nasa_power_grid_daily_2001_2022_part*" -File | ForEach-Object {
    if (Test-Path -LiteralPath (Join-Path $parts $_.Name)) { Say "  $($_.Name) is already in download_parts; the copy in the project folder stays" "Yellow" }
    else { Move-Item -LiteralPath $_.FullName -Destination $parts; Say "  moved $($_.Name) to data\raw\nasa_power_grid\download_parts" }
}
Add-Type -AssemblyName Microsoft.VisualBasic
$toDelete = Join-Path $root "_to_delete"
if (Test-Path -LiteralPath $toDelete) {
    [Microsoft.VisualBasic.FileIO.FileSystem]::DeleteDirectory($toDelete, [Microsoft.VisualBasic.FileIO.UIOption]::OnlyErrorDialogs, [Microsoft.VisualBasic.FileIO.RecycleOption]::SendToRecycleBin)
    Say "  _to_delete (the duplicate archive) sent to the Recycle Bin"
}
$mainFile = Join-Path $root "main"
if ((Test-Path -LiteralPath $mainFile) -and ((Get-Item -LiteralPath $mainFile).Length -eq 0)) {
    [Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile($mainFile, [Microsoft.VisualBasic.FileIO.UIOption]::OnlyErrorDialogs, [Microsoft.VisualBasic.FileIO.RecycleOption]::SendToRecycleBin)
    Say "  empty file 'main' sent to the Recycle Bin"
}

# ---------------------------------------------------------------- 2
Say "Step 2/9: Git repository from the bundle"
$gitCmd = Join-Path $env:ProgramFiles "Git\cmd"
if (-not (Get-Command git -ErrorAction SilentlyContinue) -and (Test-Path (Join-Path $gitCmd "git.exe"))) { $env:PATH = "$gitCmd;$env:PATH" }
if (Test-Path (Join-Path $root ".git")) { Say "  already a Git repository" }
elseif (-not (Get-Command git -ErrorAction SilentlyContinue)) { Say "  Git not found; skipped (install Git for Windows and run this again)" "Yellow" }
else {
    [void](Run "02_git_init" "git init")
    [void](Run "02_git_fetch" "git fetch .\foodshield_v2.bundle feature/workinprogress_v2:feature/workinprogress_v2")
    [void](Run "02_git_checkout" "git checkout -f feature/workinprogress_v2")
    [void](Run "02_git_remote" "git remote add origin https://github.com/bharathkuma1/FoodShield.git")
    [void](Run "02_git_status" "git status --short --branch")
}

# ---------------------------------------------------------------- 3
Say "Step 3/9: Java"
$jdk = Find-FsJava
if (-not $jdk) {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) { Say "STOP: no Java 17+ found and winget is missing. Install Temurin 21 from adoptium.net, then run this again." "Red"; Finish 1 }
    Say "  No Java 17+ found. Installing Temurin 21 with winget: accept its terms in this window and approve the Windows prompt." "Yellow"
    winget install --id EclipseAdoptium.Temurin.21.JDK -e --source winget
    $jdk = Find-FsJava
    if (-not $jdk) { Say "STOP: Java is still not found after the install. Open a new window and run this again." "Red"; Finish 1 }
}
$env:JAVA_HOME = $jdk
$env:PATH = "$jdk\bin;$env:PATH"
Say "  Java: $jdk"

# ---------------------------------------------------------------- 4
Say "Step 4/9: Hadoop native files"
# HADOOP_HOME from this window or the user/machine settings; if it is not set, suggest hadoop on the system drive.
$hh = Get-FsEnv "HADOOP_HOME"
if (-not $hh) { $hh = Join-Path ($env:SystemDrive + "\") "hadoop" }
function Hadoop-Ready($h) { Test-FsHadoop $h }
if (-not (Hadoop-Ready $hh)) {
    New-Item -ItemType Directory -Force -Path "$hh\bin" | Out-Null
    Start-Process explorer.exe "$hh\bin"
    Say "  Waiting for winutils.exe and hadoop.dll (Hadoop 3.5.0, from Bharath) in $hh\bin" "Yellow"
    while (-not (Hadoop-Ready $hh)) {
        $answer = Read-Host "Copy both files into the folder that just opened, then press Enter (or type skip to stop here)"
        if ($answer -eq "skip") { break }
    }
}
if (-not (Hadoop-Ready $hh)) { Say "STOP: Spark cannot run on Windows without the Hadoop files. Put them in $hh\bin and run this again." "Red"; Finish 1 }
$env:HADOOP_HOME = $hh
$env:PATH = "$hh\bin;$env:PATH"
if ([Environment]::GetEnvironmentVariable("HADOOP_HOME", "User") -ne $hh) { setx HADOOP_HOME $hh | Out-Null; Say "  HADOOP_HOME set to $hh for future windows" }
Say "  Hadoop: $hh"

# ---------------------------------------------------------------- 5
Say "Step 5/9: Python packages and tests (about 2 minutes)"
$pyFound = Find-FsPython
if (-not $pyFound) { Say "STOP: Python 3.11+ not found. Install Python 3.12 (with the py launcher), create the project .venv, or set FOODSHIELD_PYTHON." "Red"; Finish 1 }
$py = Format-FsPythonCommand $pyFound
Say "  Python: $($pyFound.Version) ($($pyFound.Source))"
if ((Run "05_pip" "$py -m pip install --quiet -r requirements.txt") -ne 0) { Say "STOP: pip install failed; see logs\laptop_steps\05_pip.log" "Red"; Finish 1 }
if ((Run "05_tests" "$py -m unittest discover -s tests -v") -ne 0) { Say "  Some tests failed (see logs\laptop_steps\05_tests.log); continuing, the streaming runs check themselves" "Yellow" }

# ---------------------------------------------------------------- 6
Say "Step 6/9: the four Week 6 streaming runs (about 45-60 minutes)"
Say "  If Windows asks whether Java may use the network, either answer works: everything runs on this laptop." "Yellow"
$names = "laptop_full_max", "laptop_paced_100", "laptop_paced_250", "laptop_restart"
$done = @($names | Where-Object { Test-Path (Join-Path $root "data\processed\week6_streaming\$_\week6_report.json") })
if ($done.Count -eq $names.Count) { Say "  all four laptop runs already have reports; skipped (delete data\processed\week6_streaming\laptop_* to redo)" }
else { [void](Run "06_week6_laptop" "powershell -NoProfile -ExecutionPolicy Bypass -File scripts\week6_laptop.ps1") }

# ---------------------------------------------------------------- 7
Say "Step 7/9: laptop results into the docs and the dashboard"
$applied = Run "07_apply_laptop_results" "$py scripts\apply_laptop_results.py"
if ($applied -eq 1) { Say "  a laptop run is missing; see logs\laptop_steps\06_week6_laptop.log" "Yellow"; [void]$problems.Add("a streaming run is missing") }
elseif ($applied -eq 2) { Say "  a laptop run failed a check; see docs\WEEK6_LAPTOP_RESULTS.md" "Yellow"; [void]$problems.Add("a streaming run failed a check") }

# ---------------------------------------------------------------- 8
Say "Step 8/9: demo rehearsal (the slide-11 command at 1,000 events/s)"
if (Test-Path (Join-Path $root "data\processed\week6_streaming\laptop_demo_1000\week6_report.json")) { Say "  already rehearsed; skipped" }
else {
    $t0 = Get-Date
    if ((Run "08_demo" "$py scripts\run_week6_replay.py --name laptop_demo_1000 --work-root $work --replay-from 2016-01-01 --measure-from 2022-01-01 --rate 1000") -ne 0) { [void]$problems.Add("the demo run failed (logs\laptop_steps\08_demo.log)") }
    Say ("  demo took {0:N1} minutes on this laptop" -f ((Get-Date) - $t0).TotalMinutes) "Green"
    [void](Run "08_dashboard" "$py scripts\build_week8_dashboard.py")
}

# ---------------------------------------------------------------- 9
Say "Step 9/9: batch scaling series on the laptop (optional, about 30 minutes)"
$freeGb = [math]::Round((Get-PSDrive -Name $work.Substring(0,1)).Free / 1GB, 1)
if (Test-Path (Join-Path $root "data\processed\week7_batch_scaling_laptop\summary.json")) { Say "  already done; skipped" }
elseif (-not (Test-Path (Join-Path $root "logs\laptop_steps\RUN_BATCH_SERIES"))) { Say "  skipped (optional; it ran out of memory at 186 M rows and needs a fix first)" }
elseif ($freeGb -lt 12) { Say "  skipped: only $freeGb GB free on the work folder drive (needs about 12 GB)" "Yellow" }
else {
    [void](Run "09_matplotlib" "$py -m pip install --quiet matplotlib")
    if (-not (Test-Path (Join-Path $root "data\interim\faostat_bulk\parquet"))) {
        [void](Run "09_d9a" "$py scripts\run_d9a_faostat_bulk.py --output data\processed\d9a_faostat_bulk_laptop")
    }
    if ((Run "09_batch" "$py scripts\run_week7_batch_scaling.py --phase all --output-suffix _laptop") -ne 0) { [void]$problems.Add("the batch series failed (logs\laptop_steps\09_batch.log)") }
    [void](Run "09_apply_laptop_results" "$py scripts\apply_laptop_results.py")
}

Finish 0
