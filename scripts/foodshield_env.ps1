# FoodShield: find Python, Java, Hadoop and the work folder on any Windows machine.
#
# Dot-source it from the other scripts:   . (Join-Path $PSScriptRoot "foodshield_env.ps1")
#
# Nothing here names a folder on a particular machine. Each setting can be given explicitly with an
# environment variable; otherwise it is discovered:
#
#   FOODSHIELD_PYTHON     Python command, e.g. "py -3.12" or a full path to python.exe.
#                         Otherwise: the project's .venv, then the py launcher (3.11+), then python on PATH.
#   JAVA_HOME             Java 17+ JDK. Otherwise: java on PATH, then JDKs installed under %ProgramFiles%.
#   HADOOP_HOME           Folder whose bin\ holds winutils.exe and hadoop.dll (Hadoop 3.5.0).
#                         Read from this window, then the user and machine settings.
#   FOODSHIELD_WORK_ROOT  Short folder without spaces for Kafka data and Spark checkpoints.
#                         Otherwise: fs6 on the system drive.
#
# The older names FS6_PYTHON and FS6_WORK are still read.

$script:FsRoot = Split-Path -Parent $PSScriptRoot

function Get-FsEnv($name) {
    foreach ($scope in "Process", "User", "Machine") {
        $v = [Environment]::GetEnvironmentVariable($name, $scope)
        if ($v) { return $v }
    }
    return $null
}

# ---------------------------------------------------------------- Python
function Test-FsPython($exe, [string[]]$pyArgs) {
    # Returns the version text when the command runs Python 3.11 or later, else $null.
    try { $text = (& $exe @pyArgs --version 2>&1 | Out-String).Trim() } catch { return $null }
    if ($text -match 'Python 3\.(\d+)' -and [int]$Matches[1] -ge 11) { return $text }
    return $null
}

function Find-FsPython {
    # Returns @{ Exe; Args; Version; Source } or $null.
    $tries = @()
    $explicit = Get-FsEnv "FOODSHIELD_PYTHON"
    if (-not $explicit) { $explicit = Get-FsEnv "FS6_PYTHON" }
    if ($explicit) {
        $parts = @($explicit.Trim().Split(" ", [System.StringSplitOptions]::RemoveEmptyEntries))
        if (Test-Path -LiteralPath $explicit.Trim()) { $parts = @($explicit.Trim()) }  # a full path that contains spaces
        $tries += , @{ Exe = $parts[0]; Args = @($parts | Select-Object -Skip 1); Source = "FOODSHIELD_PYTHON" }
    }
    $venv = Join-Path $script:FsRoot ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $venv) { $tries += , @{ Exe = $venv; Args = @(); Source = "project .venv" } }
    if (Get-Command py -ErrorAction SilentlyContinue) {
        foreach ($v in "-3.12", "-3.13", "-3.11", "-3") { $tries += , @{ Exe = "py"; Args = @($v); Source = "py launcher" } }
    }
    $onPath = Get-Command python -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($onPath) { $tries += , @{ Exe = $onPath.Source; Args = @(); Source = "python on PATH" } }
    foreach ($t in $tries) {
        $version = Test-FsPython $t.Exe $t.Args
        if ($version) { $t.Version = $version; return $t }
        if ($t.Source -eq "FOODSHIELD_PYTHON") { Write-Host "FOODSHIELD_PYTHON='$explicit' is not Python 3.11+; looking further." -ForegroundColor Yellow }
    }
    return $null
}

function Format-FsPythonCommand($py) {
    # One command-line string (for cmd /c), with the executable quoted.
    return (@("`"$($py.Exe)`"") + $py.Args) -join " "
}

# ---------------------------------------------------------------- Java
function Get-FsJavaMajor($javaHome) {
    $exe = Join-Path $javaHome "bin\java.exe"
    if (-not (Test-Path -LiteralPath $exe)) { return 0 }
    $text = (& $exe -version 2>&1 | Out-String)
    if ($text -match 'version "(\d+)') { return [int]$Matches[1] }
    return 0
}

function Find-FsJava {
    # Returns the home folder of a Java 17+ JDK, or $null.
    $candidates = @()
    $jh = Get-FsEnv "JAVA_HOME"
    if ($jh) { $candidates += $jh }
    $onPath = Get-Command java -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($onPath) {
        $props = (& $onPath.Source -XshowSettings:properties -version 2>&1 | Out-String)
        if ($props -match 'java\.home = (.+)') { $candidates += $Matches[1].Trim() }
    }
    foreach ($pf in @($env:ProgramFiles, ${env:ProgramFiles(x86)}) | Where-Object { $_ }) {
        foreach ($vendor in "Eclipse Adoptium", "Java", "Microsoft", "Zulu", "Amazon Corretto", "BellSoft") {
            $candidates += Get-ChildItem (Join-Path $pf $vendor) -Directory -ErrorAction SilentlyContinue |
                Sort-Object Name -Descending | ForEach-Object { $_.FullName }
        }
    }
    foreach ($c in $candidates) { if ($c -and (Get-FsJavaMajor $c) -ge 17) { return $c } }
    return $null
}

# ---------------------------------------------------------------- Hadoop
function Test-FsHadoop($h) { return [bool]($h -and (Test-Path -LiteralPath "$h\bin\winutils.exe") -and (Test-Path -LiteralPath "$h\bin\hadoop.dll")) }

function Find-FsHadoop {
    # Returns HADOOP_HOME when it holds the native files, else $null.
    $h = Get-FsEnv "HADOOP_HOME"
    if (Test-FsHadoop $h) { return $h }
    return $null
}

# ---------------------------------------------------------------- Work folder
function Get-FsWorkRoot {
    $w = Get-FsEnv "FOODSHIELD_WORK_ROOT"
    if (-not $w) { $w = Get-FsEnv "FS6_WORK" }
    if (-not $w) { $w = Join-Path ($env:SystemDrive + "\") "fs6" }
    if ($w -match '\s') { throw "The work folder '$w' contains a space; Kafka and Spark need a path without spaces. Set FOODSHIELD_WORK_ROOT." }
    return $w
}

# ---------------------------------------------------------------- All at once
function Initialize-FsEnvironment {
    # Finds Java, Hadoop and Python, puts Java and Hadoop on PATH for this window, and returns
    # @{ Python; Java; Hadoop; Work }. Stops with a clear message when something is missing.
    $java = Find-FsJava
    if (-not $java) { throw "Java 17+ not found. Install a JDK (for example Temurin 21) or set JAVA_HOME." }
    $hadoop = Find-FsHadoop
    if (-not $hadoop) { throw "Hadoop native files not found. Set HADOOP_HOME to a folder whose bin\ holds winutils.exe and hadoop.dll (Hadoop 3.5.0); see docs\PREREQUISITES.md." }
    $py = Find-FsPython
    if (-not $py) { throw "Python 3.11+ not found. Create the project .venv, install Python with the py launcher, or set FOODSHIELD_PYTHON." }
    $env:JAVA_HOME = $java
    $env:HADOOP_HOME = $hadoop
    $env:PATH = "$java\bin;$hadoop\bin;$env:PATH"
    return @{ Python = $py; Java = $java; Hadoop = $hadoop; Work = (Get-FsWorkRoot) }
}

function Stop-FsLeftoverJava {
    # Stops Kafka or Spark JVMs left over from an interrupted run of this project (on Windows they keep
    # files in the work folder locked). Only processes whose command line names this project's Kafka folder.
    $kafkaDir = Join-Path $script:FsRoot "Kafka"
    Get-CimInstance Win32_Process -Filter "Name='java.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -and $_.CommandLine.IndexOf($kafkaDir, [System.StringComparison]::OrdinalIgnoreCase) -ge 0 } |
        ForEach-Object { Write-Host "Stopping leftover FoodShield Java process $($_.ProcessId)"; Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}
