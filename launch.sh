#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if [ -x .venv/bin/python ]; then
    PYTHON=.venv/bin/python
else
    PYTHON=python3
fi

# Idempotent: no-ops on containers already healthy, so this doubles as the
# "already running" check without a separate racy status query.
echo "[launch] ensuring docker compose services are up..."
docker compose up -d --wait

# Bring the schema to head before any service touches Postgres. A no-op on an
# already-migrated volume; on a fresh one it creates the trades table the
# worker needs. set -e aborts the launch if it fails, so nothing starts
# against a half-migrated database.
echo "[launch] applying database migrations..."
$PYTHON -m alembic upgrade head

echo "[launch] starting processing worker..."
$PYTHON -m processing &
PROCESSING_PID=$!

echo "[launch] starting ingestion..."
$PYTHON main.py &
MAIN_PID=$!
CHILDREN=("$PROCESSING_PID" "$MAIN_PID")

# Runs on normal exit, an uncaught signal, or either child dying on its own -
# whichever child is still alive gets stopped so the app never limps along
# on just one half. Explicit kill rather than relying on shared process-group
# signal delivery, so this still works under a supervisor that only signals
# this script's own pid (e.g. systemd, docker).
shutdown() {
    trap - EXIT INT TERM
    for pid in "${CHILDREN[@]}"; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    wait "${CHILDREN[@]}" 2>/dev/null || true
}
trap shutdown EXIT INT TERM

set +e
wait -n "${CHILDREN[@]}"
EXIT_CODE=$?
set -e

exit "$EXIT_CODE"
