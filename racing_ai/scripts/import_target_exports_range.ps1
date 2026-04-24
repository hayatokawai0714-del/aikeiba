param(
  [Parameter(Mandatory = $true)]
  [string]$StartDate,

  [Parameter(Mandatory = $true)]
  [string]$EndDate,

  [string]$TargetExportRoot = "data\\target_exports",
  [string]$OutputRoot = "data\\raw",
  [string]$DbPath = "data\\warehouse\\aikeiba.duckdb",
  [string]$NormalizedRoot = "data\\normalized",
  [string]$FeatureSnapshotVersion = "fs_v1",
  [string]$TargetSuffix = "hist_from_target",
  [switch]$Overwrite
)

$ErrorActionPreference = "Stop"

function Validate-Date([string]$dateStr, [string]$label) {
  if ($dateStr -notmatch "^\d{4}-\d{2}-\d{2}$") {
    throw "$label must be YYYY-MM-DD. got: $dateStr"
  }
}

function Resolve-TargetExportDir([string]$rootDir, [string]$raceDate) {
  $ymd = $raceDate.Replace("-", "")
  $candidates = @(
    (Join-Path $rootDir $ymd),
    (Join-Path $rootDir "${ymd}_target_export"),
    (Join-Path $rootDir "${ymd}_target"),
    (Join-Path $rootDir $raceDate)
  )
  foreach ($c in $candidates) {
    if (-not (Test-Path $c)) { continue }
    $required = @("payouts.txt", "entries.csv", "odds_target.csv", "results.txt", "horse_master.csv")
    $ok = $true
    foreach ($rf in $required) {
      if (-not (Test-Path (Join-Path $c $rf))) {
        $ok = $false
        break
      }
    }
    if ($ok) { return $c }
  }
  return ""
}

Validate-Date $StartDate "StartDate"
Validate-Date $EndDate "EndDate"

$start = [datetime]::ParseExact($StartDate, "yyyy-MM-dd", $null)
$end = [datetime]::ParseExact($EndDate, "yyyy-MM-dd", $null)
if ($end -lt $start) {
  throw "EndDate must be >= StartDate"
}

Push-Location (Split-Path -Parent $PSCommandPath)
try {
  Set-Location (Resolve-Path "..").Path

  if (-not (Test-Path $TargetExportRoot)) {
    Write-Host "[target-import] target export root not found: $TargetExportRoot"
    Write-Host "[target-import] imported=0 skipped=0 missing_bundle=0"
    return
  }

  $imported = @()
  $missingBundle = @()

  for ($d = $start; $d -le $end; $d = $d.AddDays(1)) {
    $ds = $d.ToString("yyyy-MM-dd")
    $ymd = $d.ToString("yyyyMMdd")
    $targetDir = Resolve-TargetExportDir -rootDir $TargetExportRoot -raceDate $ds
    if ([string]::IsNullOrWhiteSpace($targetDir)) {
      $missingBundle += $ds
      continue
    }

    $snapshot = "${ymd}_${TargetSuffix}"
    $rawDir = Join-Path $OutputRoot $snapshot
    $oddsSnapshot = "odds_target_${ymd}"

    Write-Host ""
    Write-Host "[target-import] $ds"
    Write-Host "  target_dir=$targetDir"
    Write-Host "  raw_dir=$rawDir"

    $args = @(
      "build-raw-from-target-export",
      "--target-dir", $targetDir,
      "--raw-dir", $rawDir,
      "--race-date", $ds,
      "--snapshot-version", $snapshot,
      "--odds-snapshot-version", $oddsSnapshot
    )
    if ($Overwrite) { $args += "--overwrite" }
    & aikeiba @args

    aikeiba jv-file-pipeline `
      --db-path $DbPath `
      --raw-dir $rawDir `
      --normalized-root $NormalizedRoot `
      --race-date $ds `
      --snapshot-version $snapshot | Out-Null

    aikeiba build-features `
      --db-path $DbPath `
      --race-date $ds `
      --feature-snapshot-version $FeatureSnapshotVersion | Out-Null

    $imported += $ds
  }

  Write-Host ""
  Write-Host "[target-import] imported=$($imported.Count) missing_bundle=$($missingBundle.Count)"
  if ($missingBundle.Count -gt 0) {
    Write-Host "[target-import] missing target bundle dates (first 30):"
    $missingBundle | Select-Object -First 30 | ForEach-Object { Write-Host "  - $_" }
  }
}
finally {
  Pop-Location
}

