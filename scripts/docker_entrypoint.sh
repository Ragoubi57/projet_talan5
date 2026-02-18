#!/bin/sh
set -e

# Ensure data products exist before starting the app.
set +e
python - <<'PY'
import os
import sys

import duckdb

path = os.environ.get("DUCKDB_PATH", "/app/data/warehouse.duckdb")
if not os.path.exists(path):
    sys.exit(1)

con = duckdb.connect(path)
tables = {r[0] for r in con.execute("show tables").fetchall()}
required = {"dp_complaints", "dp_call_reports"}
missing = required - tables
needs_rebuild = bool(missing)
if not needs_rebuild:
    def has_col(table, col):
        cols = {r[0] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
        return col in cols
    # Ensure new regional columns exist
    if not has_col("dp_complaints", "region"):
        needs_rebuild = True
    if not has_col("dp_call_reports", "bank_region"):
        needs_rebuild = True
con.close()
sys.exit(1 if needs_rebuild else 0)
PY
status=$?
set -e

if [ $status -ne 0 ]; then
  echo "Data products missing. Running ingestion pipeline..."
  python scripts/ingest.py
fi

exec "$@"
