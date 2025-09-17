#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -f target/h2-oracle-sync-1.0.1.jar ]; then
  echo "[build] packaging..."
  mvn -q -DskipTests package
fi
out="backups/h2-backup-$(date +%Y%m%d-%H%M%S).zip"
echo "[backup] writing to $out"
exec java -jar target/h2-oracle-sync-1.0.1.jar --backup --backup.dir=backups --backup.file="$(basename "$out")"
