param(
  # Command-line parsing for `-File` is strict; easiest is a single comma-separated token:
  #   -RaceDates "2026-03-06,2026-03-13,2026-03-20,2026-03-27"
  # We also accept array input when called from an interactive PowerShell session.
  [Parameter(Mandatory = $true)]
  [string[]]$RaceDates,

  [string]$DbPath = "data\\warehouse\\aikeiba.duckdb",
  [string]$FeatureSnapshotVersion = "fs_v1"
)

$ErrorActionPreference = "Stop"

function Require-Command([string]$name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    throw "Required command not found: $name"
  }
}

function Validate-Date([string]$dateStr) {
  if ($dateStr -notmatch "^\d{4}-\d{2}-\d{2}$") {
    throw "RaceDates must be YYYY-MM-DD. got: $dateStr"
  }
}

Require-Command "python"

if ($RaceDates.Count -eq 1 -and $RaceDates[0] -match ",") {
  $RaceDates = $RaceDates[0].Split(",", [System.StringSplitOptions]::RemoveEmptyEntries) |
    ForEach-Object { $_.Trim() }
}

$RaceDates | ForEach-Object { Validate-Date $_ }

Push-Location (Split-Path -Parent $PSCommandPath)
try {
  Set-Location (Resolve-Path "..").Path  # repo root = ...\racing_ai

  if (-not (Test-Path $DbPath)) {
    throw "DB not found: $DbPath"
  }

  # Ensure JSON is always an array (even for 1 element).
  $datesJson = ConvertTo-Json -InputObject @($RaceDates) -Compress

  $py = @"
import json
import duckdb

db_path = r'''$DbPath'''
dates = json.loads(r'''$datesJson''')
fs_ver = r'''$FeatureSnapshotVersion'''

con = duckdb.connect(db_path)
try:
    for d in dates:
        # Collect race_ids first to avoid accidental over-delete.
        race_ids = [r[0] for r in con.execute("select race_id from races where race_date = ?", (d,)).fetchall()]
        if not race_ids:
            print(f"[purge] {d}: no races found, skip")
            continue

        # Delete child tables by race_id.
        con.execute("delete from entries where race_id in (select unnest(?))", (race_ids,))
        con.execute("delete from results where race_id in (select unnest(?))", (race_ids,))
        con.execute("delete from payouts where race_id in (select unnest(?))", (race_ids,))
        con.execute("delete from odds where race_id in (select unnest(?))", (race_ids,))

        # Delete feature_store by race_date+version.
        con.execute("delete from feature_store where race_date = ? and feature_snapshot_version = ?", (d, fs_ver))

        # Finally delete races.
        con.execute("delete from races where race_date = ?", (d,))
        print(f"[purge] {d}: deleted races={len(race_ids)}")
finally:
    con.close()
"@

  $tmpPy = Join-Path $env:TEMP "aikeiba_purge_race_dates.py"
  Set-Content -Path $tmpPy -Value $py -Encoding UTF8
  & python $tmpPy | Out-Host
}
finally {
  Pop-Location
}
