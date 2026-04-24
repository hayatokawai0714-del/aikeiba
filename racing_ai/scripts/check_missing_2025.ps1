Param(
  [Parameter(Mandatory = $false)]
  [string]$DbPath = "data\\warehouse\\aikeiba.duckdb",

  [Parameter(Mandatory = $false)]
  [string]$FeatureSnapshotVersion = "fs_v1"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Format-Date([int]$month, [int]$day) {
  return ("2025-{0:00}-{1:00}" -f $month, $day)
}

$expected = @()

# 2025 race days (user-provided calendar)
$expected += 5,6,11,12,13,18,19,25,26 | ForEach-Object { Format-Date 1 $_ }
$expected += 1,2,8,9,10,15,16,22,23 | ForEach-Object { Format-Date 2 $_ }
$expected += 1,2,8,9,15,16,22,23,29,30 | ForEach-Object { Format-Date 3 $_ }
$expected += 5,6,12,13,19,20,26,27 | ForEach-Object { Format-Date 4 $_ }
$expected += 3,4,10,11,17,18,24,25,31 | ForEach-Object { Format-Date 5 $_ }
$expected += 1,7,8,14,15,21,22,28,29 | ForEach-Object { Format-Date 6 $_ }
$expected += 5,6,12,13,19,20,26,27 | ForEach-Object { Format-Date 7 $_ }
$expected += 2,3,9,10,16,17,23,24,30,31 | ForEach-Object { Format-Date 8 $_ }
$expected += 6,7,13,14,15,20,21,27,28 | ForEach-Object { Format-Date 9 $_ }
$expected += 4,5,11,12,13,18,19,25,26 | ForEach-Object { Format-Date 10 $_ }
$expected += 1,2,8,9,15,16,22,23,24,29,30 | ForEach-Object { Format-Date 11 $_ }
$expected += 6,7,13,14,20,21,27,28 | ForEach-Object { Format-Date 12 $_ }

$expected = $expected | Sort-Object -Unique

Write-Host ("[check-missing-2025] expected_dates={0}" -f $expected.Count)
Write-Host ("[check-missing-2025] db={0} snapshot={1}" -f $DbPath, $FeatureSnapshotVersion)
Write-Host ""

& (Join-Path $PSScriptRoot "check_expected_race_dates.ps1") `
  -ExpectedRaceDates $expected `
  -DbPath $DbPath `
  -FeatureSnapshotVersion $FeatureSnapshotVersion
