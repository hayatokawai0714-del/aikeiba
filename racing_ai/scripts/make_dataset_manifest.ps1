param(
  [Parameter(Mandatory = $true)]
  [string]$DatasetName,

  [Parameter(Mandatory = $true)]
  [string]$TrainPeriod,

  [Parameter(Mandatory = $true)]
  [string]$ValidPeriod,

  [Parameter(Mandatory = $true)]
  [string]$TestPeriod,

  [string]$DbPath = "data\warehouse\aikeiba.duckdb",
  [string]$OutDir = "data\datasets",
  [string]$TaskName = "top3",
  [string]$FeatureSnapshotVersion = "fs_v1",
  [string]$FiltersJson = "{}",
  [string]$ExcludedRulesJson = "[]"
)

$ErrorActionPreference = "Stop"

function Require-Command([string]$name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    throw "Required command not found: $name"
  }
}

function Validate-Period([string]$period, [string]$label) {
  if ($period -notmatch "^\d{4}-\d{2}-\d{2}\.\.\d{4}-\d{2}-\d{2}$") {
    throw "$label must be YYYY-MM-DD..YYYY-MM-DD. got: $period"
  }
}

Require-Command "aikeiba"
Validate-Period $TrainPeriod "TrainPeriod"
Validate-Period $ValidPeriod "ValidPeriod"
Validate-Period $TestPeriod "TestPeriod"

Push-Location (Split-Path -Parent $PSCommandPath)
try {
  # repo root = ...\racing_ai
  Set-Location (Resolve-Path "..").Path

  Write-Host "[make-dataset-manifest] dataset_name=$DatasetName"
  Write-Host "[make-dataset-manifest] train=$TrainPeriod valid=$ValidPeriod test=$TestPeriod"

  aikeiba make-dataset-manifest `
    --db-path $DbPath `
    --out-dir $OutDir `
    --dataset-name $DatasetName `
    --task-name $TaskName `
    --feature-snapshot-version $FeatureSnapshotVersion `
    --train-period $TrainPeriod `
    --valid-period $ValidPeriod `
    --test-period $TestPeriod `
    --filters-json $FiltersJson `
    --excluded-rules-json $ExcludedRulesJson

  $manifestPath = Join-Path $OutDir (Join-Path $DatasetName "dataset_manifest.json")
  Write-Host "[make-dataset-manifest] OK -> $manifestPath"
}
finally {
  Pop-Location
}

