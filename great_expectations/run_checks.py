"""Lightweight Great Expectations-style quality checks.

Uses DuckDB directly for quality assertions (since full GE requires complex setup).
This implements the same checks that would be in a GE suite.
"""
import os
import sys
import json
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import duckdb

DB_PATH = os.environ.get("DUCKDB_PATH", os.path.join(PROJECT_ROOT, "data", "warehouse.duckdb"))


def run_checks():
    """Run quality checks on data products."""
    print("=== Great Expectations Quality Checks ===\n")

    conn = duckdb.connect(DB_PATH, read_only=True)
    results = {"timestamp": datetime.now(timezone.utc).isoformat(), "checks": [], "all_passed": True}

    # dp_complaints checks
    checks = [
        ("dp_complaints: not_null complaint_id",
         "SELECT COUNT(*) FROM dp_complaints WHERE complaint_id IS NULL", 0),
        ("dp_complaints: not_null date_received",
         "SELECT COUNT(*) FROM dp_complaints WHERE date_received IS NULL", 0),
        ("dp_complaints: not_null product",
         "SELECT COUNT(*) FROM dp_complaints WHERE product IS NULL", 0),
        ("dp_complaints: not_null company",
         "SELECT COUNT(*) FROM dp_complaints WHERE company IS NULL", 0),
        ("dp_complaints: unique complaint_id",
         "SELECT COUNT(*) - COUNT(DISTINCT complaint_id) FROM dp_complaints", 0),
        ("dp_complaints: timely_response values",
         "SELECT COUNT(*) FROM dp_complaints WHERE timely_response NOT IN ('Yes', 'No')", 0),
        ("dp_complaints: row_count > 0",
         "SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM dp_complaints", 0),

        # dp_call_reports checks
        ("dp_call_reports: not_null quarter",
         "SELECT COUNT(*) FROM dp_call_reports WHERE quarter IS NULL", 0),
        ("dp_call_reports: not_null bank_name",
         "SELECT COUNT(*) FROM dp_call_reports WHERE bank_name IS NULL", 0),
        ("dp_call_reports: not_null bank_id",
         "SELECT COUNT(*) FROM dp_call_reports WHERE bank_id IS NULL", 0),
        ("dp_call_reports: not_null total_assets",
         "SELECT COUNT(*) FROM dp_call_reports WHERE total_assets IS NULL", 0),
        ("dp_call_reports: positive total_assets",
         "SELECT COUNT(*) FROM dp_call_reports WHERE total_assets <= 0", 0),
        ("dp_call_reports: row_count > 0",
         "SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM dp_call_reports", 0),
        ("dp_call_reports: tier1_capital_ratio range",
         "SELECT COUNT(*) FROM dp_call_reports WHERE tier1_capital_ratio < 0 OR tier1_capital_ratio > 100", 0),
    ]

    for name, sql, expected in checks:
        try:
            result = conn.execute(sql).fetchone()[0]
            passed = result == expected
            status = "✅ PASS" if passed else "❌ FAIL"
            if not passed:
                results["all_passed"] = False
            results["checks"].append({
                "name": name,
                "passed": passed,
                "actual": result,
                "expected": expected,
            })
            print(f"{status} {name} (actual={result}, expected={expected})")
        except Exception as e:
            results["all_passed"] = False
            results["checks"].append({"name": name, "passed": False, "error": str(e)})
            print(f"❌ FAIL {name} (error: {e})")

    conn.close()

    # Write results
    results_path = os.path.join(PROJECT_ROOT, "artifacts", "ge_results.json")
    os.makedirs(os.path.dirname(results_path), exist_ok=True)
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\nOverall: {'✅ ALL PASSED' if results['all_passed'] else '❌ SOME CHECKS FAILED'}")
    return results["all_passed"]


if __name__ == "__main__":
    passed = run_checks()
    sys.exit(0 if passed else 1)
