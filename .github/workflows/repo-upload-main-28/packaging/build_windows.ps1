param(
  [switch]$BuildInstaller
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$distDir = Join-Path $root "dist"
$buildDir = Join-Path $root "build"
$requirementsFile = Join-Path $root "requirements.txt"
$serverScript = Join-Path $PSScriptRoot "dayingjing_server_main.py"
$launcherScript = Join-Path $PSScriptRoot "dayingjing_launcher.py"
$launcherName = "DaYingJingLauncher"
$pythonCommand = if (Get-Command py -ErrorAction SilentlyContinue) {
  "py"
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
  "python"
} else {
  throw "Python launcher not found. Install Python 3 first."
}

Write-Host ""
Write-Host "Installing build dependencies..." -ForegroundColor Yellow
& $pythonCommand -m pip install -r $requirementsFile pyinstaller

Write-Host ""
Write-Host "Cleaning old build output..." -ForegroundColor Yellow
if (Test-Path $distDir) { Remove-Item -LiteralPath $distDir -Recurse -Force }
if (Test-Path $buildDir) { Remove-Item -LiteralPath $buildDir -Recurse -Force }

Write-Host ""
Write-Host "Building DaYingJingServer.exe ..." -ForegroundColor Yellow
& $pythonCommand -m PyInstaller --noconfirm --clean --onefile --console `
  --name DaYingJingServer `
  --distpath $distDir `
  --workpath (Join-Path $buildDir "server") `
  --specpath (Join-Path $buildDir "spec") `
  --hidden-import pypdf `
  --hidden-import pymupdf `
  --hidden-import fitz `
  --add-data "$root\index.html;." `
  --add-data "$root\app.js;." `
  --add-data "$root\styles.css;." `
  --add-data "$root\data\profile-overrides.json;data" `
  $serverScript

Write-Host ""
Write-Host "Building DaYingJingLauncher.exe ..." -ForegroundColor Yellow
& $pythonCommand -m PyInstaller --noconfirm --clean --onefile --windowed `
  --name $launcherName `
  --distpath $distDir `
  --workpath (Join-Path $buildDir "launcher") `
  --specpath (Join-Path $buildDir "spec") `
  $launcherScript

$serverExe = Join-Path $distDir "DaYingJingServer.exe"
$launcherExe = Join-Path $distDir "$launcherName.exe"

if (-not (Test-Path $serverExe) -or -not (Test-Path $launcherExe)) {
  throw "Build failed: missing executable output."
}

Write-Host ""
Write-Host "Build output ready:" -ForegroundColor Green
Write-Host "  $launcherExe"
Write-Host "  $serverExe"

if (-not $BuildInstaller) {
  exit 0
}

$candidates = @(
  "$env:LOCALAPPDATA\Programs\Inno Setup 6\ISCC.exe",
  "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe",
  "${env:ProgramFiles}\Inno Setup 6\ISCC.exe"
)
$iscc = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $iscc) {
  throw "Inno Setup not found. Install Inno Setup 6 first, then rerun with -BuildInstaller."
}

Write-Host ""
Write-Host "Building installer ..." -ForegroundColor Yellow
& $iscc (Join-Path $PSScriptRoot "dayingjing_installer.iss")

Write-Host ""
Write-Host "Installer build completed." -ForegroundColor Green
