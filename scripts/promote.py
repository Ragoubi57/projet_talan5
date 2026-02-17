"""Promote data products - mark as certified and queryable only if quality checks pass."""
import os
import sys
import json
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import duckdb

DB_PATH = os.environ.get("DUCKDB_PATH", os.path.join(PROJECT_ROOT, "data", "warehouse.duckdb"))


def promote_data_products(dbt_passed: bool = True, ge_passed: bool = True):
    """Mark data products as promoted if quality checks pass."""
    conn = duckdb.connect(DB_PATH)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS promote_status (
            data_product VARCHAR PRIMARY KEY,
            promoted BOOLEAN DEFAULT FALSE,
            last_promoted VARCHAR,
            dbt_passed BOOLEAN DEFAULT FALSE,
            ge_passed BOOLEAN DEFAULT FALSE
        )
    """)

    data_products = ["dp_complaints", "dp_call_reports"]

    for dp in data_products:
        promoted = dbt_passed and ge_passed
        timestamp = datetime.now(timezone.utc).isoformat()

        # Check if table exists
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {dp}").fetchone()[0]
            table_exists = True
        except Exception:
            table_exists = False
            promoted = False

        conn.execute("""
            INSERT OR REPLACE INTO promote_status VALUES (?, ?, ?, ?, ?)
        """, [dp, promoted, timestamp if promoted else None, dbt_passed, ge_passed])

        status = "✅ PROMOTED" if promoted else "❌ NOT PROMOTED"
        reason = ""
        if not table_exists:
            reason = " (table does not exist)"
        elif not dbt_passed:
            reason = " (dbt tests failed)"
        elif not ge_passed:
            reason = " (GE checks failed)"

        print(f"{dp}: {status}{reason}")

    conn.close()

    # Write promote artifact
    artifact_path = os.path.join(PROJECT_ROOT, "artifacts", "promote_status.json")
    os.makedirs(os.path.dirname(artifact_path), exist_ok=True)
    with open(artifact_path, 'w') as f:
        json.dump({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dbt_passed": dbt_passed,
            "ge_passed": ge_passed,
            "promoted": dbt_passed and ge_passed,
        }, f, indent=2)

    return dbt_passed and ge_passed


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dbt-failed", action="store_true", help="Simulate dbt test failure")
    parser.add_argument("--ge-failed", action="store_true", help="Simulate GE check failure")
    args = parser.parse_args()

    promote_data_products(
        dbt_passed=not args.dbt_failed,
        ge_passed=not args.ge_failed,
    )
