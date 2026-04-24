param(
  [Parameter(Mandatory = $true)]
  [string]$StartDate,

  [Parameter(Mandatory = $true)]
  [string]$EndDate,

  # Root dir for outputs. Each day becomes: <OutputRoot>\YYYYMMDD_hist_from_jv\
  [string]$OutputRoot = "data\\raw",

  # Suffix in output dir name: YYYYMMDD_<Suffix>
  [string]$Suffix = "hist_from_jv",

  [switch]$Overwrite,
  [switch]$LogVerbose,

  # Keep days where races.csv has 0 data rows.
  [switch]$KeepEmpty,

  # Optional: run Aikeiba raw->normalized->warehouse after export.
  [switch]$Ingest,

  [string]$DbPath = "data\\warehouse\\aikeiba.duckdb",
  [string]$NormalizedRoot = "data\\normalized",

  # Override JV-Link local data directory (where *.jvd live).
  # Typical: C:\ProgramData\JRA-VAN\Data Lab
  [string]$JvDataDir = "",

  # Force rebuild exporter even when exe already exists.
  [switch]$RebuildExporter,

  # If set, keep only days that contain at least one JRA Central venue.
  # This prevents keeping local/overseas races that may appear in JV data on non-weekend days.
  [switch]$CentralOnly
)

$ErrorActionPreference = "Stop"

function Require-Command([string]$name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    throw "Required command not found: $name"
  }
}

function Validate-Date([string]$dateStr, [string]$label) {
  if ($dateStr -notmatch "^\d{4}-\d{2}-\d{2}$") {
    throw "$label must be YYYY-MM-DD. got: $dateStr"
  }
}

function Get-CsvDataRows([string]$path) {
  if (-not (Test-Path $path)) { return 0 }
  $lines = (Get-Content $path | Measure-Object -Line).Lines
  if ($lines -le 1) { return 0 }
  return ($lines - 1)
}

function Get-LatestWriteTimeUtc([string[]]$paths) {
  $latest = [datetime]::MinValue
  foreach ($p in $paths) {
    if (-not (Test-Path $p)) { continue }
    $item = Get-Item $p
    if ($item.LastWriteTimeUtc -gt $latest) {
      $latest = $item.LastWriteTimeUtc
    }
  }
  return $latest
}

function Has-JraCentralVenue([string]$racesCsvPath) {
  if (-not (Test-Path $racesCsvPath)) { return $false }
  try {
    # JRA central venue codes (01..10): 札幌, 函館, 福島, 新潟, 東京, 中山, 中京, 京都, 阪神, 小倉
    # races.csv from JV exporter may have mojibake in `surface`, but `venue_code` is stable numeric.
    $jraVenueCodes = @("01", "02", "03", "04", "05", "06", "07", "08", "09", "10")
    $rows = Import-Csv -Path $racesCsvPath
    foreach ($r in $rows) {
      if ($null -eq $r.venue_code) { continue }
      $vc = ($r.venue_code).ToString().Trim().PadLeft(2, '0')
      if ($jraVenueCodes -contains $vc) { return $true }
    }
    return $false
  }
  catch {
    # If parsing fails, do not delete aggressively.
    Write-Host "[export-jv] WARN: failed to parse races.csv for venue check: $racesCsvPath"
    return $true
  }
}

Validate-Date $StartDate "StartDate"
Validate-Date $EndDate "EndDate"

Require-Command "dotnet"
Require-Command "aikeiba"

Push-Location (Split-Path -Parent $PSCommandPath)
try {
  # repo root = ...\racing_ai
  Set-Location (Resolve-Path "..").Path

  $csproj = "tools\\jvlink_direct_exporter\\Aikeiba.JVLinkDirectExporter.csproj"
  $exe = "tools\\jvlink_direct_exporter\\bin\\Release\\net8.0-windows\\win-x86\\Aikeiba.JVLinkDirectExporter.exe"
  $exporterSources = @(
    $csproj,
    "tools\\jvlink_direct_exporter\\Program.cs"
  )
  $needBuild = $RebuildExporter -or -not (Test-Path $exe)
  if (-not $needBuild) {
    $exeTime = (Get-Item $exe).LastWriteTimeUtc
    $srcTime = Get-LatestWriteTimeUtc $exporterSources
    if ($srcTime -gt $exeTime) {
      $needBuild = $true
    }
  }

  if ($needBuild) {
    Write-Host "[export-jv] building exporter (win-x86 Release)..."
    dotnet build $csproj -c Release -r win-x86 | Out-Null
  }
  if (-not (Test-Path $exe)) {
    throw "exporter exe not found after build: $exe"
  }

  $start = [datetime]::ParseExact($StartDate, "yyyy-MM-dd", $null)
  $end = [datetime]::ParseExact($EndDate, "yyyy-MM-dd", $null)
  if ($end -lt $start) { throw "EndDate must be >= StartDate" }

  for ($d = $start; $d -le $end; $d = $d.AddDays(1)) {
    $dateStr = $d.ToString("yyyy-MM-dd")
    $ymd = $d.ToString("yyyyMMdd")
    $snap = "${ymd}_${Suffix}"
    $outDir = Join-Path $OutputRoot $snap

    Write-Host ""
    Write-Host "=== export $dateStr -> $outDir ==="

    $args = @("--race-date", $dateStr, "--output-dir", $outDir)
    if ($Overwrite) { $args += "--overwrite" }
    if ($LogVerbose) { $args += "--verbose" }
    if ($JvDataDir -and $JvDataDir.Trim().Length -gt 0) { $args += @("--jv-data-dir", $JvDataDir) }

    & $exe @args

    $racesCsv = Join-Path $outDir "races.csv"
    $raceRows = Get-CsvDataRows $racesCsv
    if ($raceRows -eq 0 -and -not $KeepEmpty) {
      Write-Host "[export-jv] no races (rows=0). removing: $outDir"
      Remove-Item -Recurse -Force $outDir
      continue
    }

    if ($CentralOnly -and -not (Has-JraCentralVenue $racesCsv)) {
      Write-Host "[export-jv] non-central venues only. removing: $outDir"
      Remove-Item -Recurse -Force $outDir
      continue
    }

    Write-Host "[export-jv] races_rows=$raceRows"

    if ($Ingest) {
      Write-Host "[export-jv] ingest pipeline: raw -> normalized -> warehouse"
      aikeiba jv-file-pipeline `
        --db-path $DbPath `
        --raw-dir $outDir `
        --normalized-root $NormalizedRoot `
        --race-date $dateStr `
        --snapshot-version $snap | Out-Null
      Write-Host "[export-jv] ingest OK"
    }
  }
}
finally {
  Pop-Location
}
