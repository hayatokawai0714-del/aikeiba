param(
  [Parameter(Mandatory = $true)]
  [string]$RaceDate,

  [Parameter(Mandatory = $true)]
  [string]$SourceDir,

  [string]$OutRawDir = "",

  [switch]$Inspect
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path
$srcRoot = Join-Path $repoRoot "src"

$yyyyMMdd = (Get-Date $RaceDate).ToString("yyyyMMdd")
if ($OutRawDir -eq "") {
  $OutRawDir = Join-Path $repoRoot ("data\\raw\\{0}_real" -f $yyyyMMdd)
}

$sourceAbs = (Resolve-Path $SourceDir).Path
$outAbs = (Resolve-Path (Split-Path -Parent $OutRawDir) -ErrorAction SilentlyContinue)
if (-not $outAbs) {
  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutRawDir) | Out-Null
}

Write-Host "[INFO] source_dir=$sourceAbs"
Write-Host "[INFO] out_raw_dir=$OutRawDir"

Push-Location $repoRoot
try {
  powershell -NoProfile -File (Join-Path $repoRoot "tools\\aikeiba_cli.ps1") build-real-raw-from-jv `
    --source-dir $sourceAbs `
    --target-date $RaceDate `
    --out-raw-dir $OutRawDir

  if ($Inspect) {
    powershell -NoProfile -File (Join-Path $repoRoot "tools\\aikeiba_cli.ps1") inspect-raw-dir --raw-dir $OutRawDir
  }
}
finally {
  Pop-Location
}
