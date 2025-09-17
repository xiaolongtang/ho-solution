#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
echo "[build] packaging..."
mvn -q -DskipTests package
echo "[run] starting app..."
exec java -jar target/h2-oracle-sync-1.0.1.jar
