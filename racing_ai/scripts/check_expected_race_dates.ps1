param(
  [Parameter(Mandatory = $true)]
  [string[]]$ExpectedRaceDates,

  [string]$DbPath = "data\\warehouse\\aikeiba.duckdb",
  [string]$FeatureSnapshotVersion = "fs_v1",
  [string]$OutputRoot = "data\\raw",
  [string]$Suffix = "hist_from_jv"
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

foreach ($d in $ExpectedRaceDates) {
  Validate-Date $d
}
Require-Command "python"

Push-Location (Split-Path -Parent $PSCommandPath)
try {
  Set-Location (Resolve-Path "..").Path

  if (-not (Test-Path $DbPath)) {
    throw "DB not found: $DbPath"
  }

  $datesJson = ($ExpectedRaceDates | Sort-Object -Unique | ConvertTo-Json -Compress)
  $py = @"
import json
from datetime import date
import duckdb

db_path = r'''$DbPath'''
fs_ver = r'''$FeatureSnapshotVersion'''
expected = json.loads(r'''$datesJson''')

con = duckdb.connect(db_path, read_only=True)
try:
    rows = []
    for ds in expected:
        races = con.execute("select count(*) from races where race_date = ?", (ds,)).fetchone()[0]
        entries = con.execute("select count(*) from entries e join races r on r.race_id=e.race_id where r.race_date = ?", (ds,)).fetchone()[0]
        results = con.execute("select count(*) from results rr join races r on r.race_id=rr.race_id where r.race_date = ?", (ds,)).fetchone()[0]
        payouts = con.execute("select count(*) from payouts p join races r on r.race_id=p.race_id where r.race_date = ?", (ds,)).fetchone()[0]
        odds = con.execute("select count(*) from odds o join races r on r.race_id=o.race_id where r.race_date = ?", (ds,)).fetchone()[0]
        feats = con.execute("select count(*) from feature_store where race_date = ? and feature_snapshot_version = ?", (ds, fs_ver)).fetchone()[0]
        rows.append({
            "race_date": ds,
            "races": int(races),
            "entries": int(entries),
            "results": int(results),
            "payouts": int(payouts),
            "odds": int(odds),
            "feature_store_rows": int(feats),
        })
    print(json.dumps({"rows": rows}, ensure_ascii=False))
finally:
    con.close()
"@

  $tmpPy = Join-Path $env:TEMP "aikeiba_check_expected_race_dates.py"
  Set-Content -Path $tmpPy -Value $py -Encoding UTF8
  $json = & python $tmpPy
  $obj = $json | ConvertFrom-Json

  $table = $obj.rows | ForEach-Object {
    [PSCustomObject]@{
      race_date = $_.race_date
      races = $_.races
      entries = $_.entries
      results = $_.results
      payouts = $_.payouts
      odds = $_.odds
      feature_store_rows = $_.feature_store_rows
      status = if ([int]$_.races -gt 0) { "present" } else { "missing" }
    }
  }

  Write-Host "[check-expected] snapshot=$FeatureSnapshotVersion db=$DbPath"
  $table | Format-Table -AutoSize

  $missing = $table | Where-Object { $_.status -eq "missing" } | Select-Object -ExpandProperty race_date
  $present = $table | Where-Object { $_.status -eq "present" } | Select-Object -ExpandProperty race_date

  Write-Host ""
  Write-Host "[check-expected] present=$($present.Count) missing=$($missing.Count)"
  if ($missing.Count -gt 0) {
    Write-Host "[check-expected] missing race dates:"
    $missing | ForEach-Object { Write-Host "  - $_" }
  }

  Write-Host ""
  Write-Host "[check-expected] raw dirs with races.csv>0"
  foreach ($ds in $ExpectedRaceDates | Sort-Object -Unique) {
    $key = $ds.Replace("-", "")
    $dir = Join-Path $OutputRoot "${key}_${Suffix}"
    $rows = Get-CsvDataRows (Join-Path $dir "races.csv")
    if ($rows -gt 0) {
      Write-Host "  + $dir (races_rows=$rows)"
    }
  }
}
finally {
  Pop-Location
}
