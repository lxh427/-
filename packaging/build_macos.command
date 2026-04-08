#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cd "$PROJECT_ROOT"
chmod +x "$PROJECT_ROOT/packaging/build_macos.sh"
./packaging/build_macos.sh
