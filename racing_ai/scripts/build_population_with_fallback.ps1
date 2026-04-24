param(
  [Parameter(Mandatory = $true)]
  [string[]]$ExpectedRaceDates,

  [string]$StartDate = "",
  [string]$EndDate = "",

  [string]$OutputRoot = "data\\raw",
  [string]$JvSuffix = "hist_from_jv",
  [string]$TargetSuffix = "hist_from_target",

  [string]$DbPath = "data\\warehouse\\aikeiba.duckdb",
  [string]$NormalizedRoot = "data\\normalized",
  [string]$FeatureSnapshotVersion = "fs_v1",

  [string]$TargetExportRoot = "data\\target_exports",

  [switch]$SkipJvExport,
  [switch]$Overwrite,
  [switch]$LogVerbose
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

function Get-MissingRaceDates(
  [string]$DbPathParam,
  [string]$FeatureSnapshotVersionParam,
  [string[]]$ExpectedDatesParam
) {
  $datesJson = ($ExpectedDatesParam | Sort-Object -Unique | ConvertTo-Json -Compress)
  $py = @"
import json
import duckdb

db_path = r'''$DbPathParam'''
fs_ver = r'''$FeatureSnapshotVersionParam'''
expected = json.loads(r'''$datesJson''')

con = duckdb.connect(db_path, read_only=True)
try:
    missing = []
    for ds in expected:
        races = con.execute("select count(*) from races where race_date = ?", (ds,)).fetchone()[0]
        if int(races) <= 0:
            missing.append(ds)
    print(json.dumps({"missing": missing}, ensure_ascii=False))
finally:
    con.close()
"@
  $tmpPy = Join-Path $env:TEMP "aikeiba_build_population_with_fallback_missing.py"
  Set-Content -Path $tmpPy -Value $py -Encoding UTF8
  $json = & python $tmpPy
  $obj = $json | ConvertFrom-Json
  return @($obj.missing)
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
    $allOk = $true
    foreach ($rf in $required) {
      if (-not (Test-Path (Join-Path $c $rf))) {
        $allOk = $false
        break
      }
    }
    if ($allOk) {
      return $c
    }
  }
  return ""
}

foreach ($d in $ExpectedRaceDates) {
  Validate-Date $d "ExpectedRaceDates[]"
}

Require-Command "powershell"
Require-Command "python"
Require-Command "aikeiba"

Push-Location (Split-Path -Parent $PSCommandPath)
try {
  Set-Location (Resolve-Path "..").Path

  $expected = @($ExpectedRaceDates | Sort-Object -Unique)
  if ($expected.Count -eq 0) {
    throw "ExpectedRaceDates is empty."
  }

  if ([string]::IsNullOrWhiteSpace($StartDate)) {
    $StartDate = ($expected | Sort-Object | Select-Object -First 1)
  }
  if ([string]::IsNullOrWhiteSpace($EndDate)) {
    $EndDate = ($expected | Sort-Object | Select-Object -Last 1)
  }
  Validate-Date $StartDate "StartDate"
  Validate-Date $EndDate "EndDate"

  if (-not $SkipJvExport) {
    $jvScript = Join-Path $PWD.Path "scripts\\make_population_from_jv_range.ps1"
    if (-not (Test-Path $jvScript)) {
      throw "Missing script: $jvScript"
    }

    Write-Host "[build-pop-fallback] step1 JV export+ingest range: $StartDate .. $EndDate"
    $jvArgs = @(
      "-NoProfile",
      "-ExecutionPolicy", "Bypass",
      "-File", $jvScript,
      "-StartDate", $StartDate,
      "-EndDate", $EndDate,
      "-OutputRoot", $OutputRoot,
      "-Suffix", $JvSuffix,
      "-DbPath", $DbPath,
      "-NormalizedRoot", $NormalizedRoot,
      "-FeatureSnapshotVersion", $FeatureSnapshotVersion
    )
    if ($Overwrite) { $jvArgs += "-Overwrite" }
    if ($LogVerbose) { $jvArgs += "-LogVerbose" }
    & powershell @jvArgs
  }
  else {
    Write-Host "[build-pop-fallback] step1 skipped (SkipJvExport=true)"
  }

  if (-not (Test-Path $DbPath)) {
    throw "DB not found: $DbPath"
  }

  Write-Host ""
  Write-Host "[build-pop-fallback] step2 detect missing race dates in DB"
  $missing = @(Get-MissingRaceDates -DbPathParam $DbPath -FeatureSnapshotVersionParam $FeatureSnapshotVersion -ExpectedDatesParam $expected)
  Write-Host "[build-pop-fallback] expected=$($expected.Count) missing_before=$($missing.Count)"
  if ($missing.Count -eq 0) {
    Write-Host "[build-pop-fallback] all expected race dates are present."
    return
  }

  Write-Host "[build-pop-fallback] missing dates:"
  $missing | ForEach-Object { Write-Host "  - $_" }

  Write-Host ""
  Write-Host "[build-pop-fallback] step3 TARGET fallback from root: $TargetExportRoot"

  $fallbackDone = @()
  $fallbackSkipped = @()
  foreach ($ds in $missing) {
    $targetDir = Resolve-TargetExportDir -rootDir $TargetExportRoot -raceDate $ds
    if ([string]::IsNullOrWhiteSpace($targetDir)) {
      Write-Host "[build-pop-fallback] skip $ds (target bundle not found)"
      $fallbackSkipped += $ds
      continue
    }

    $ymd = $ds.Replace("-", "")
    $snapshot = "${ymd}_${TargetSuffix}"
    $rawDir = Join-Path $OutputRoot $snapshot
    $oddsSnap = "odds_target_${ymd}"

    Write-Host ""
    Write-Host "[build-pop-fallback] TARGET build $ds"
    Write-Host "  target_dir=$targetDir"
    Write-Host "  raw_dir=$rawDir"

    aikeiba build-raw-from-target-export `
      --target-dir $targetDir `
      --raw-dir $rawDir `
      --race-date $ds `
      --snapshot-version $snapshot `
      --odds-snapshot-version $oddsSnap `
      --overwrite

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

    $fallbackDone += $ds
  }

  Write-Host ""
  Write-Host "[build-pop-fallback] fallback_done=$($fallbackDone.Count) fallback_skipped=$($fallbackSkipped.Count)"

  Write-Host ""
  Write-Host "[build-pop-fallback] step4 re-check missing"
  $missingAfter = @(Get-MissingRaceDates -DbPathParam $DbPath -FeatureSnapshotVersionParam $FeatureSnapshotVersion -ExpectedDatesParam $expected)
  Write-Host "[build-pop-fallback] missing_after=$($missingAfter.Count)"
  if ($missingAfter.Count -gt 0) {
    $missingAfter | ForEach-Object { Write-Host "  - $_" }
  }
}
finally {
  Pop-Location
}
