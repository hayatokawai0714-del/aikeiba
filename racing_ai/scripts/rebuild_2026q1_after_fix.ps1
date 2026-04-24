param(
  [Parameter(Mandatory = $false)]
  [string]$StartDate = "2026-01-01",

  [Parameter(Mandatory = $false)]
  [string]$EndDate = "2026-03-31",

  [Parameter(Mandatory = $false)]
  [string]$DbPath = "data\\warehouse\\aikeiba.duckdb",

  [Parameter(Mandatory = $false)]
  [string]$RawRoot = "data\\raw",

  [Parameter(Mandatory = $false)]
  [string]$NormalizedRoot = "data\\normalized",

  [Parameter(Mandatory = $false)]
  [string]$SnapshotVersion = ("fix_" + (Get-Date -Format "yyyyMMdd_HHmmss"))
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function ToYmd([string]$d) {
  return ($d -replace "-", "")
}

function ParseDate([string]$d) {
  return [DateTime]::ParseExact($d, "yyyy-MM-dd", $null)
}

$start = ParseDate $StartDate
$end = ParseDate $EndDate
if ($end -lt $start) { throw "EndDate must be >= StartDate" }

Write-Host "[rebuild-2026q1] range=$StartDate..$EndDate snapshot=$SnapshotVersion"

$rawDirs =
  Get-ChildItem $RawRoot -Directory -Filter "2026*_hist_from_jv" |
  Where-Object {
    $name = $_.Name
    if ($name.Length -lt 8) { return $false }
    $ymd = $name.Substring(0, 8)
    if ($ymd -notmatch "^[0-9]{8}$") { return $false }
    $dt = [DateTime]::ParseExact($ymd, "yyyyMMdd", $null)
    return ($dt -ge $start -and $dt -le $end)
  } |
  Sort-Object Name

Write-Host "[rebuild-2026q1] raw days found: $($rawDirs.Count)"
if ($rawDirs.Count -eq 0) {
  Write-Warning "[rebuild-2026q1] no raw dirs found; nothing to do"
  exit 0
}

foreach ($dir in $rawDirs) {
  $ymd = $dir.Name.Substring(0, 8)
  $raceDate = "{0}-{1}-{2}" -f $ymd.Substring(0, 4), $ymd.Substring(4, 2), $ymd.Substring(6, 2)
  Write-Host ""
  Write-Host "=== normalize+ingest $raceDate ($($dir.FullName)) ==="

  aikeiba normalize-raw-jv `
    --db-path $DbPath `
    --raw-dir $dir.FullName `
    --normalized-root $NormalizedRoot `
    --race-date $raceDate `
    --snapshot-version $SnapshotVersion | Out-Host

  $normDir = Join-Path $NormalizedRoot $SnapshotVersion
  $normDir = Join-Path $normDir $raceDate
  if (!(Test-Path $normDir)) {
    throw "normalized dir not found: $normDir"
  }

  aikeiba ingest-normalized `
    --db-path $DbPath `
    --normalized-dir $normDir `
    --race-date $raceDate `
    --snapshot-version $SnapshotVersion | Out-Host
}

Write-Host ""
Write-Host "[rebuild-2026q1] done."

