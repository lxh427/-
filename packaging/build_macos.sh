#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"
BUILD_DIR="$ROOT_DIR/build"
REQUIREMENTS_FILE="$ROOT_DIR/requirements.txt"
SERVER_SCRIPT="$ROOT_DIR/packaging/dayingjing_server_main.py"
LAUNCHER_SCRIPT="$ROOT_DIR/packaging/dayingjing_launcher.py"
LAUNCHER_NAME="达盈镜v1.0-mac"
APP_BUNDLE="$DIST_DIR/${LAUNCHER_NAME}.app"

python3 -m pip install -r "$REQUIREMENTS_FILE" pyinstaller

rm -rf "$DIST_DIR" "$BUILD_DIR"

python3 -m PyInstaller --noconfirm --clean --onefile --console \
  --name DaYingJingServer \
  --distpath "$DIST_DIR" \
  --workpath "$BUILD_DIR/server" \
  --specpath "$BUILD_DIR/spec" \
  --hidden-import pypdf \
  --hidden-import pymupdf \
  --hidden-import fitz \
  --add-data "$ROOT_DIR/index.html:." \
  --add-data "$ROOT_DIR/app.js:." \
  --add-data "$ROOT_DIR/styles.css:." \
  --add-data "$ROOT_DIR/data/profile-overrides.json:data" \
  "$SERVER_SCRIPT"

python3 -m PyInstaller --noconfirm --clean --windowed \
  --name "$LAUNCHER_NAME" \
  --distpath "$DIST_DIR" \
  --workpath "$BUILD_DIR/launcher" \
  --specpath "$BUILD_DIR/spec" \
  "$LAUNCHER_SCRIPT"

if [[ ! -d "$APP_BUNDLE" ]]; then
  echo "达盈镜v1.0-mac.app not found. Run this script on macOS with a GUI-capable Python/PyInstaller environment."
  exit 1
fi

cp "$DIST_DIR/DaYingJingServer" "$APP_BUNDLE/Contents/Resources/DaYingJingServer"

cd "$DIST_DIR"
zip -qry "${LAUNCHER_NAME}.zip" "${LAUNCHER_NAME}.app"

echo ""
echo "Build output ready:"
echo "  $APP_BUNDLE"
echo "  $DIST_DIR/${LAUNCHER_NAME}.zip"
