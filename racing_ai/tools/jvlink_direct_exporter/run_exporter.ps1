param(
  [string]$RaceDate = "",
  [string]$StartDate = "",
  [string]$EndDate = "",
  [string]$RawRoot = "data/raw",
  [switch]$Overwrite,
  [switch]$VerboseLog,
  [string]$FromTime = "",
  [string]$DataSpec = "RACE",
  [string]$Option = "0",
  [string]$OddsSnapshotVersion = "",
  [switch]$DryRun
)

function Resolve-Date([string]$d) {
  if ($d -eq "") { return $null }
  return (Get-Date $d)
}

if ($RaceDate -eq "" -and $StartDate -eq "" -and $EndDate -eq "") {
  throw "Specify -RaceDate or (-StartDate and -EndDate)."
}
if ($RaceDate -ne "" -and ($StartDate -ne "" -or $EndDate -ne "")) {
  throw "Use either -RaceDate or (-StartDate and -EndDate), not both."
}
if (($StartDate -ne "" -and $EndDate -eq "") -or ($StartDate -eq "" -and $EndDate -ne "")) {
  throw "Specify both -StartDate and -EndDate."
}

Write-Host "[INFO] dotnet build tools/jvlink_direct_exporter/Aikeiba.JVLinkDirectExporter.csproj -c Release -r win-x86"
if (-not $DryRun) {
  dotnet build tools/jvlink_direct_exporter/Aikeiba.JVLinkDirectExporter.csproj -c Release -r win-x86 | Out-Host
}

$exe = "tools/jvlink_direct_exporter/bin/Release/net8.0-windows/win-x86/Aikeiba.JVLinkDirectExporter.exe"
if (-not (Test-Path $exe)) {
  throw "Exporter exe not found: $exe"
}

function Invoke-One([datetime]$dt) {
  $yyyyMMdd = $dt.ToString("yyyyMMdd")
  $raceDateArg = $dt.ToString("yyyy-MM-dd")
  $outDir = Join-Path $RawRoot "${yyyyMMdd}_real"
  $oddsSv = $OddsSnapshotVersion
  if ($oddsSv -eq "") { $oddsSv = "odds_jv_${yyyyMMdd}" }

  $args = @(
    "--race-date", $raceDateArg,
    "--output-dir", $outDir,
    "--dataspec", $DataSpec,
    "--option", $Option,
    "--odds-snapshot-version", $oddsSv
  )

  if ($Overwrite) { $args += "--overwrite" }
  if ($VerboseLog) { $args += "--verbose" }
  if ($FromTime -ne "") { $args += @("--fromtime", $FromTime) }
  if ($DryRun) { $args += "--dry-run" }

  Write-Host "[INFO] $exe $($args -join ' ')"
  if (-not $DryRun) {
    & $exe @args | Out-Host
  }
}

if ($RaceDate -ne "") {
  Invoke-One (Resolve-Date $RaceDate)
  exit 0
}

$start = Resolve-Date $StartDate
$end = Resolve-Date $EndDate
if ($start -gt $end) { throw "StartDate must be <= EndDate" }

for ($d = $start; $d -le $end; $d = $d.AddDays(1)) {
  Invoke-One $d
}
