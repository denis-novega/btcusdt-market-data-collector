#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if command -v open >/dev/null 2>&1; then
  open btcusdt_live_viewer.html
elif command -v xdg-open >/dev/null 2>&1; then
  xdg-open btcusdt_live_viewer.html
else
  echo "Open btcusdt_live_viewer.html in your browser."
fi
