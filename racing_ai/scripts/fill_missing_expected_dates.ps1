param(
  [Parameter(Mandatory = $true)]
  [string[]]$ExpectedRaceDates,

  [string]$DbPath = "data\\warehouse\\aikeiba.duckdb",
  [string]$NormalizedRoot = "data\\normalized",
  [string]$FeatureSnapshotVersion = "fs_v1",
  [string]$OutputRoot = "data\\raw",
  [string]$Suffix = "hist_from_jv",
  [string]$TargetFixSuffix = "hist_from_target_fix",
  [string]$TargetExportRoot = "data\\target_exports",
  [switch]$OverwriteRaw,
  [switch]$LogVerbose,
  [string]$JvDataDir = "C:\\ProgramData\\JRA-VAN\\Data Lab",
  [switch]$UseTargetFallback,
  [switch]$WriteMissingChecklist
)

$ErrorActionPreference = "Stop"

function Require-Command([string]$name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    throw "Required command not found: $name"
  }
}

function Validate-Date([string]$dateStr) {
  if ($dateStr -notmatch "^\d{4}-\d{2}-\d{2}$") {
    throw "ExpectedRaceDates must be YYYY-MM-DD. got: $dateStr"
  }
}

function Get-CsvDataRows([string]$path) {
  if (-not (Test-Path $path)) { return 0 }
  $lines = (Get-Content $path | Measure-Object -Line).Lines
  if ($lines -le 1) { return 0 }
  return ($lines - 1)
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

function Test-JvCacheExistsForDate([string]$jvRootDir, [string]$raceDate) {
  if ([string]::IsNullOrWhiteSpace($jvRootDir)) { return $false }
  if (-not (Test-Path $jvRootDir)) { return $false }
  $ymd = $raceDate.Replace("-", "")
  $pattern = "*${ymd}*.jvd"
  return [bool](Get-ChildItem $jvRootDir -Recurse -File -Filter $pattern -ErrorAction SilentlyContinue | Select-Object -First 1)
}

Require-Command "python"
Require-Command "powershell"
Require-Command "aikeiba"

if ($ExpectedRaceDates.Count -eq 1 -and $ExpectedRaceDates[0] -match ",") {
  $ExpectedRaceDates = $ExpectedRaceDates[0].Split(",", [System.StringSplitOptions]::RemoveEmptyEntries) |
    ForEach-Object { $_.Trim() }
}

$ExpectedRaceDates = $ExpectedRaceDates | Sort-Object -Unique
$ExpectedRaceDates | ForEach-Object { Validate-Date $_ }

Push-Location (Split-Path -Parent $PSCommandPath)
try {
  Set-Location (Resolve-Path "..").Path  # repo root = ...\racing_ai

  if (-not (Test-Path $DbPath)) {
    throw "DB not found: $DbPath"
  }

  $datesJson = ConvertTo-Json -InputObject @($ExpectedRaceDates) -Compress
  $py = @"
import json
import duckdb

db_path = r'''$DbPath'''
expected = json.loads(r'''$datesJson''')
con = duckdb.connect(db_path, read_only=True)
try:
    missing = []
    for ds in expected:
        races = con.execute("select count(*) from races where race_date = ?", (ds,)).fetchone()[0]
        if races == 0:
            missing.append(ds)
    print(json.dumps({"missing": missing}, ensure_ascii=False))
finally:
    con.close()
"@

  $tmpPy = Join-Path $env:TEMP "aikeiba_fill_missing_expected_dates.py"
  Set-Content -Path $tmpPy -Value $py -Encoding UTF8
  $result = (& python $tmpPy) | ConvertFrom-Json
  $missing = @($result.missing)

  Write-Host "[fill-missing] expected=$($ExpectedRaceDates.Count) missing_before=$($missing.Count)"
  if ($missing.Count -eq 0) {
    Write-Host "[fill-missing] nothing to do."
    return
  }

  $exportScript = Join-Path $PWD.Path "scripts\\export_jv_raw_range.ps1"
  if (-not (Test-Path $exportScript)) {
    throw "Missing script: $exportScript"
  }

  $resolved = New-Object System.Collections.Generic.List[string]
  $unresolved = New-Object System.Collections.Generic.List[string]

  foreach ($d in $missing) {
    Write-Host ""
    Write-Host "=== fill $d ==="

    $exportArgs = @(
      "-NoProfile",
      "-ExecutionPolicy", "Bypass",
      "-File", $exportScript,
      "-StartDate", $d,
      "-EndDate", $d,
      "-OutputRoot", $OutputRoot,
      "-Suffix", $Suffix,
      "-DbPath", $DbPath,
      "-NormalizedRoot", $NormalizedRoot,
      "-Ingest",
      "-CentralOnly"
    )
    if ($OverwriteRaw) { $exportArgs += "-Overwrite" }
    if ($LogVerbose) { $exportArgs += "-LogVerbose" }
    if (Test-Path $JvDataDir) {
      $exportArgs += "-JvDataDir"
      $exportArgs += $JvDataDir
    }

    & powershell @exportArgs

    $key = $d.Replace("-", "")
    $rawDir = Join-Path $OutputRoot "${key}_${Suffix}"
    $rows = Get-CsvDataRows (Join-Path $rawDir "races.csv")
    if ($rows -gt 0) {
      aikeiba build-features `
        --db-path $DbPath `
        --race-date $d `
        --feature-snapshot-version $FeatureSnapshotVersion | Out-Host
      $resolved.Add($d) | Out-Null
      Write-Host "[fill-missing] resolved $d (races_rows=$rows)"
    }
    else {
      $unresolved.Add($d) | Out-Null
      Write-Host "[fill-missing] unresolved $d (races_rows=0)"
    }
  }

  Write-Host ""
  Write-Host "[fill-missing] resolved=$($resolved.Count) unresolved=$($unresolved.Count)"

  if ($UseTargetFallback -and $unresolved.Count -gt 0) {
    Write-Host ""
    Write-Host "[fill-missing] target fallback start (suffix=$TargetFixSuffix)"

    $stillUnresolved = New-Object System.Collections.Generic.List[string]
    $resolvedByTarget = New-Object System.Collections.Generic.List[string]
    $unresolvedReason = @{}

    foreach ($d in $unresolved) {
      $key = $d.Replace("-", "")
      $rawDir = Join-Path $OutputRoot "${key}_${TargetFixSuffix}"
      $racesPath = Join-Path $rawDir "races.csv"
      $rows = Get-CsvDataRows $racesPath
      $hasJvCache = Test-JvCacheExistsForDate -jvRootDir $JvDataDir -raceDate $d

      if ($rows -le 0) {
        $bundleDir = Resolve-TargetExportDir -rootDir $TargetExportRoot -raceDate $d
        if (-not [string]::IsNullOrWhiteSpace($bundleDir)) {
          $snapshot = "${key}_${TargetFixSuffix}"
          $oddsSnapshot = "odds_target_${key}"
          if (-not (Test-Path $rawDir)) {
            New-Item -ItemType Directory -Path $rawDir -Force | Out-Null
          }

          Write-Host "[fill-missing] build target raw $d from $bundleDir"
          $buildArgs = @(
            "build-raw-from-target-export",
            "--target-dir", $bundleDir,
            "--raw-dir", $rawDir,
            "--race-date", $d,
            "--snapshot-version", $snapshot,
            "--odds-snapshot-version", $oddsSnapshot
          )
          if ($OverwriteRaw) { $buildArgs += "--overwrite" }
          & aikeiba @buildArgs | Out-Host
          $rows = Get-CsvDataRows $racesPath
        }
        else {
          if ($hasJvCache) {
            $unresolvedReason[$d] = "target_bundle_not_found"
          }
          else {
            $unresolvedReason[$d] = "jv_cache_missing_and_target_bundle_not_found"
          }
        }
      }

      if ($rows -gt 0) {
        Write-Host "[fill-missing] ingest/build from target fallback: $d (races_rows=$rows)"
        aikeiba ingest-raw-dir `
          --db-path $DbPath `
          --raw-dir $rawDir `
          --normalized-root $NormalizedRoot | Out-Host

        aikeiba build-features `
          --db-path $DbPath `
          --race-date $d `
          --feature-snapshot-version $FeatureSnapshotVersion | Out-Host

        $resolvedByTarget.Add($d) | Out-Null
      }
      else {
        if (-not $unresolvedReason.ContainsKey($d)) {
          if ($hasJvCache) {
            $unresolvedReason[$d] = "raw_dir_has_no_races_csv_or_zero_rows"
          }
          else {
            $unresolvedReason[$d] = "jv_cache_missing_and_raw_dir_has_no_races_csv_or_zero_rows"
          }
        }
        $stillUnresolved.Add($d) | Out-Null
      }
    }

    Write-Host "[fill-missing] target fallback resolved=$($resolvedByTarget.Count) unresolved=$($stillUnresolved.Count)"
    if ($stillUnresolved.Count -gt 0) {
      Write-Host "[fill-missing] unresolved reasons:"
      foreach ($d in $stillUnresolved) {
        Write-Host "  - $d : $($unresolvedReason[$d])"
      }
    }
    $unresolved = $stillUnresolved
  }

  if ($unresolved.Count -gt 0) {
    Write-Host "[fill-missing] unresolved dates:"
    $unresolved | ForEach-Object { Write-Host "  - $_" }
  }

  if ($WriteMissingChecklist) {
    $checkDir = Join-Path $OutputRoot "_checklists"
    if (-not (Test-Path $checkDir)) {
      New-Item -ItemType Directory -Path $checkDir -Force | Out-Null
    }
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $csvPath = Join-Path $checkDir "missing_expected_dates_${stamp}.csv"
    $rows = @()
    foreach ($d in $ExpectedRaceDates) {
      $status = if ($unresolved -contains $d) { "missing" } else { "present_or_resolved" }
      $rows += [pscustomobject]@{
        race_date = $d
        status = $status
      }
    }
    $rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvPath
    Write-Host "[fill-missing] wrote checklist: $csvPath"
  }

  # Final summary for expected dates.
  $checkScript = Join-Path $PWD.Path "scripts\\check_expected_race_dates.ps1"
  if (Test-Path $checkScript) {
    Write-Host ""
    Write-Host "[fill-missing] final check"
    & $checkScript `
      -ExpectedRaceDates $ExpectedRaceDates `
      -DbPath $DbPath `
      -FeatureSnapshotVersion $FeatureSnapshotVersion `
      -OutputRoot $OutputRoot `
      -Suffix $Suffix
  }
}
finally {
  Pop-Location
}
