param(
  [Parameter(Mandatory = $true)]
  [string]$StartDate,

  [Parameter(Mandatory = $true)]
  [string]$EndDate,

  [string]$OutputRoot = "data\\raw",
  [string]$Suffix = "hist_from_jv",

  [switch]$Overwrite,
  [switch]$LogVerbose,

  [string]$DbPath = "data\\warehouse\\aikeiba.duckdb",
  [string]$NormalizedRoot = "data\\normalized",
  [string]$FeatureSnapshotVersion = "fs_v1"
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

Validate-Date $StartDate "StartDate"
Validate-Date $EndDate "EndDate"
Require-Command "powershell"
Require-Command "aikeiba"

Push-Location (Split-Path -Parent $PSCommandPath)
try {
  Set-Location (Resolve-Path "..").Path  # repo root = ...\racing_ai

  $exportScript = Join-Path $PWD.Path "scripts\\export_jv_raw_range.ps1"
  if (-not (Test-Path $exportScript)) {
    throw "Missing script: $exportScript"
  }

  $exportArgs = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $exportScript,
    "-StartDate", $StartDate,
    "-EndDate", $EndDate,
    "-OutputRoot", $OutputRoot,
    "-Suffix", $Suffix,
    "-DbPath", $DbPath,
    "-NormalizedRoot", $NormalizedRoot,
    "-Ingest",
    "-CentralOnly"
  )
  if ($Overwrite) { $exportArgs += "-Overwrite" }
  if ($LogVerbose) { $exportArgs += "-LogVerbose" }
  # Prefer Data Lab folder if user has it (this matches where JV-Link cache files are often stored).
  $defaultJvDataDir = "C:\\ProgramData\\JRA-VAN\\Data Lab"
  if (Test-Path $defaultJvDataDir) {
    $exportArgs += "-JvDataDir"
    $exportArgs += $defaultJvDataDir
  }

  Write-Host "[make-population] export+ingest: $StartDate .. $EndDate"
  & powershell @exportArgs
  Write-Host "[make-population] export+ingest done"

  $start = [datetime]::ParseExact($StartDate, "yyyy-MM-dd", $null)
  $end = [datetime]::ParseExact($EndDate, "yyyy-MM-dd", $null)

  $pattern = "*_${Suffix}"
  $dirs = Get-ChildItem -Directory $OutputRoot -Filter $pattern -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match "^\d{8}_" } |
    Sort-Object Name

  $datesToBuild = @()
  foreach ($dir in $dirs) {
    $ymd = $dir.Name.Substring(0, 8)
    $d = [datetime]::ParseExact($ymd, "yyyyMMdd", $null)
    if ($d -lt $start -or $d -gt $end) { continue }

    $racesCsv = Join-Path $dir.FullName "races.csv"
    if ((Get-CsvDataRows $racesCsv) -le 0) { continue }
    $datesToBuild += $d.ToString("yyyy-MM-dd")
  }

  if ($datesToBuild.Count -eq 0) {
    Write-Host "[make-population] no race days found under $OutputRoot ($pattern) within range."
    return
  }

  Write-Host "[make-population] build-features for $($datesToBuild.Count) race days (feature_snapshot_version=$FeatureSnapshotVersion)"
  foreach ($dateStr in $datesToBuild) {
    Write-Host ""
    Write-Host "=== build-features $dateStr ==="
    aikeiba build-features `
      --db-path $DbPath `
      --race-date $dateStr `
      --feature-snapshot-version $FeatureSnapshotVersion | Out-Null
  }

  Write-Host ""
  Write-Host "[make-population] OK. Built features for:"
  $datesToBuild | ForEach-Object { Write-Host "  - $_" }
}
finally {
  Pop-Location
}
