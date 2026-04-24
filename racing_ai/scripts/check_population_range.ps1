param(
  [Parameter(Mandatory = $true)]
  [string]$StartDate,

  [Parameter(Mandatory = $true)]
  [string]$EndDate,

  [string]$OutputRoot = "data\\raw",
  [string]$Suffix = "hist_from_jv",

  [string]$DbPath = "data\\warehouse\\aikeiba.duckdb",
  [string]$FeatureSnapshotVersion = "fs_v1",

  # Optional: also run `aikeiba inspect-raw-dir` for each raw dir found.
  [switch]$InspectRaw
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
Require-Command "python"

Push-Location (Split-Path -Parent $PSCommandPath)
try {
  Set-Location (Resolve-Path "..").Path  # repo root = ...\racing_ai

  $start = [datetime]::ParseExact($StartDate, "yyyy-MM-dd", $null)
  $end = [datetime]::ParseExact($EndDate, "yyyy-MM-dd", $null)
  if ($end -lt $start) { throw "EndDate must be >= StartDate" }

  $pattern = "*_${Suffix}"
  $dirs = Get-ChildItem -Directory $OutputRoot -Filter $pattern -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match "^\d{8}_" } |
    Sort-Object Name

  $rawDirs = @()
  foreach ($dir in $dirs) {
    $ymd = $dir.Name.Substring(0, 8)
    $d = [datetime]::ParseExact($ymd, "yyyyMMdd", $null)
    if ($d -lt $start -or $d -gt $end) { continue }

    $racesCsv = Join-Path $dir.FullName "races.csv"
    if ((Get-CsvDataRows $racesCsv) -le 0) { continue }
    $rawDirs += $dir.FullName
  }

  Write-Host "[check-population] range=$StartDate..$EndDate suffix=$Suffix"
  Write-Host "[check-population] race days found: $($rawDirs.Count)"
  if ($rawDirs.Count -gt 0) {
    $rawDirs | ForEach-Object { Write-Host "  - $_" }
  }

  if ($InspectRaw) {
    Require-Command "aikeiba"
    foreach ($d in $rawDirs) {
      Write-Host ""
      Write-Host "=== inspect-raw-dir $d ==="
      aikeiba inspect-raw-dir --raw-dir $d | Out-Host
    }
  }

  if (-not (Test-Path $DbPath)) {
    Write-Host ""
    Write-Host "[check-population] DB not found: $DbPath"
    return
  }

  $py = @"
import json
import sys
from datetime import date, timedelta

import duckdb

db_path = r'''$DbPath'''
start_date = date.fromisoformat(r'''$StartDate''')
end_date = date.fromisoformat(r'''$EndDate''')
fs_ver = r'''$FeatureSnapshotVersion'''

def q1(con, sql, params=()):
    return con.execute(sql, params).fetchone()[0]

con = duckdb.connect(db_path, read_only=True)
try:
    out = []
    d = start_date
    while d <= end_date:
        ds = d.isoformat()
        races = q1(con, "select count(*) from races where race_date = ?", (ds,))
        if races == 0:
            d += timedelta(days=1)
            continue
        entries = q1(con, "select count(*) from entries e join races r on r.race_id=e.race_id where r.race_date = ?", (ds,))
        results = q1(con, "select count(*) from results rr join races r on r.race_id=rr.race_id where r.race_date = ?", (ds,))
        payouts = q1(con, "select count(*) from payouts p join races r on r.race_id=p.race_id where r.race_date = ?", (ds,))
        odds = q1(con, "select count(*) from odds o join races r on r.race_id=o.race_id where r.race_date = ?", (ds,))
        feats = q1(con, "select count(*) from feature_store where race_date = ? and feature_snapshot_version = ?", (ds, fs_ver))
        out.append({
            "race_date": ds,
            "races": int(races),
            "entries": int(entries),
            "results": int(results),
            "payouts": int(payouts),
            "odds": int(odds),
            "feature_store_rows": int(feats),
            "feature_snapshot_version": fs_ver,
        })
        d += timedelta(days=1)
    print(json.dumps({"db_path": db_path, "rows": out}, ensure_ascii=False, indent=2))
finally:
    con.close()
"@

  Write-Host ""
  Write-Host "[check-population] warehouse/feature_store counts:"
  $tmpPy = Join-Path $env:TEMP "aikeiba_check_population_range.py"
  Set-Content -Path $tmpPy -Value $py -Encoding UTF8
  $json = & python $tmpPy
  $obj = $json | ConvertFrom-Json
  $obj.rows | ForEach-Object {
    [PSCustomObject]@{
      race_date = $_.race_date
      races = $_.races
      entries = $_.entries
      results = $_.results
      payouts = $_.payouts
      odds = $_.odds
      feature_store_rows = $_.feature_store_rows
      feature_snapshot_version = $_.feature_snapshot_version
    }
  } | Format-Table -AutoSize
}
finally {
  Pop-Location
}
