#!/bin/sh
set -e
# Build assets if dist missing or empty
if [ ! -d dist ] || [ "$(find dist -maxdepth 1 -type f | wc -l)" -eq 0 ]; then
  echo "[entrypoint] Building dist assets..."
  npm run build || {
    echo "[entrypoint] Build failed" >&2
    exit 1
  }
fi
exec "$@"