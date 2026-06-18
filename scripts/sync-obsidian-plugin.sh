#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_MAIN="$ROOT_DIR/main.js"
DIST_MANIFEST="$ROOT_DIR/manifest.json"
DIST_VERSIONS="$ROOT_DIR/versions.json"

TARGET_DIR="${1:-$HOME/Documents/Obsidian Vault/.obsidian/plugins/walus-storage-obsidian}"

if [[ ! -d "$ROOT_DIR/node_modules" ]]; then
  (cd "$ROOT_DIR" && npm install)
fi

(cd "$ROOT_DIR" && npm run build)

if [[ ! -f "$DIST_MAIN" ]]; then
  echo "missing: $DIST_MAIN" >&2
  exit 1
fi

mkdir -p "$TARGET_DIR"
cp -f "$DIST_MAIN" "$TARGET_DIR/main.js"

if [[ -f "$DIST_MANIFEST" ]]; then
  cp -f "$DIST_MANIFEST" "$TARGET_DIR/manifest.json"
fi

if [[ -f "$DIST_VERSIONS" ]]; then
  cp -f "$DIST_VERSIONS" "$TARGET_DIR/versions.json"
fi

echo "synced to: $TARGET_DIR"
