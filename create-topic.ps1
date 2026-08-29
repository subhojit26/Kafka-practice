<#
.SYNOPSIS
  Option B: create the 'library-events' topic from the HOST using the Kafka CLI
  against the EXTERNAL listeners (localhost:9092/9094/9096).

.DESCRIPTION
  - Requires Java on PATH or JAVA_HOME (you already have a JDK for this project).
  - Downloads the Apache Kafka CLI locally (into .kafka-cli) if not already present.
    Nothing is installed system-wide.
  - Runs kafka-topics.bat from the host, so localhost:9092/9094/9096 correctly
    map to the three published broker ports.

.EXAMPLE
  ./create-topic.ps1
  ./create-topic.ps1 -Topic library-events -Partitions 3 -ReplicationFactor 3
#>
param(
    [string]$Topic = "library-events",
    [int]$Partitions = 3,
    [int]$ReplicationFactor = 3,
    [string]$BootstrapServer = "localhost:9092",
    [string]$KafkaVersion = "3.7.1",
    [string]$ScalaVersion = "2.13"
)

$ErrorActionPreference = "Stop"

# --- Verify Java is available -------------------------------------------------
$java = (Get-Command java -ErrorAction SilentlyContinue)
if (-not $java -and -not $env:JAVA_HOME) {
    throw "Java not found. Ensure 'java' is on PATH or set JAVA_HOME (you already have a JDK for this project)."
}

# --- Ensure the Kafka CLI is available locally --------------------------------
# NOTE: The Windows kafka-*.bat scripts build a huge -classpath from every jar
# in libs\. A deep install path pushes the command line past cmd.exe's 8191-char
# limit ("The input line is too long."). Installing under a SHORT root avoids it.
$cliDir    = "C:\kafka-cli"
$distName  = "kafka_${ScalaVersion}-${KafkaVersion}"
$distDir   = Join-Path $cliDir $distName
$topicsBat = Join-Path $distDir "bin\windows\kafka-topics.bat"

if (-not (Test-Path $topicsBat)) {
    New-Item -ItemType Directory -Force -Path $cliDir | Out-Null
    $tgz = Join-Path $cliDir "$distName.tgz"
    $url = "https://archive.apache.org/dist/kafka/$KafkaVersion/$distName.tgz"

    Write-Host "Downloading Kafka CLI $KafkaVersion (one-time) ..." -ForegroundColor Cyan
    Invoke-WebRequest -Uri $url -OutFile $tgz

    Write-Host "Extracting ..." -ForegroundColor Cyan
    # tar ships with Windows 10+.
    tar -xzf $tgz -C $cliDir

    Remove-Item $tgz -Force
}

if (-not (Test-Path $topicsBat)) {
    throw "kafka-topics.bat not found at $topicsBat after extraction."
}

# --- Resolve java executable --------------------------------------------------
if ($env:JAVA_HOME) {
    $javaExe = Join-Path $env:JAVA_HOME "bin\java.exe"
} elseif ($java) {
    $javaExe = $java.Source
} else {
    throw "Unable to resolve a java executable."
}

# The Windows kafka-*.bat scripts enumerate every jar into -classpath, which can
# blow past cmd.exe's 8191-char limit. Invoke Java directly with a libs\* wildcard
# instead: the JVM expands it, so the command line stays short.
$libs = Join-Path $distDir "libs\*"

# --- Create the topic ---------------------------------------------------------
Write-Host "Creating topic '$Topic' (partitions=$Partitions, RF=$ReplicationFactor) via $BootstrapServer ..." -ForegroundColor Green
& $javaExe -cp "$libs" org.apache.kafka.tools.TopicCommand `
    --bootstrap-server $BootstrapServer --create --if-not-exists `
    --topic $Topic --partitions $Partitions --replication-factor $ReplicationFactor

# --- Describe to confirm ------------------------------------------------------
Write-Host "`nDescribing topic '$Topic' ..." -ForegroundColor Green
& $javaExe -cp "$libs" org.apache.kafka.tools.TopicCommand `
    --bootstrap-server $BootstrapServer --describe --topic $Topic



