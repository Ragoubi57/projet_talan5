"""Validate data files against their JSON schemas."""
import os
import sys
import json
import csv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


def validate_csv_against_schema(csv_path: str, schema: dict, sample_size: int = 1000) -> dict:
    """Validate a CSV file against a JSON schema. Returns validation results."""
    results = {"valid": True, "errors": [], "rows_checked": 0, "quarantined": 0}
    required = schema.get("required", [])
    properties = schema.get("properties", {})

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= sample_size:
                break
            results["rows_checked"] += 1

            # Check required fields
            for field in required:
                if field not in row or not row[field] or row[field].strip() == '':
                    results["errors"].append(f"Row {i+1}: missing required field '{field}'")
                    results["quarantined"] += 1
                    results["valid"] = False
                    break

    return results


def main():
    from catalog.loader import load_schema
    data_dir = os.path.join(PROJECT_ROOT, "data")

    print("=== Schema Validation ===\n")

    # Validate complaints
    complaints_csv = os.path.join(data_dir, "complaints.csv")
    if os.path.exists(complaints_csv):
        schema = load_schema("complaints")
        result = validate_csv_against_schema(complaints_csv, schema)
        status = "✅ PASSED" if result["valid"] else "❌ FAILED"
        print(f"complaints.csv: {status}")
        print(f"  Rows checked: {result['rows_checked']}")
        print(f"  Quarantined: {result['quarantined']}")
        if result["errors"]:
            for e in result["errors"][:5]:
                print(f"  Error: {e}")
    else:
        print("complaints.csv: ⚠️ NOT FOUND")

    # Validate call reports
    call_reports_csv = os.path.join(data_dir, "call_reports.csv")
    if os.path.exists(call_reports_csv):
        schema = load_schema("call_reports")
        result = validate_csv_against_schema(call_reports_csv, schema)
        status = "✅ PASSED" if result["valid"] else "❌ FAILED"
        print(f"\ncall_reports.csv: {status}")
        print(f"  Rows checked: {result['rows_checked']}")
        print(f"  Quarantined: {result['quarantined']}")
        if result["errors"]:
            for e in result["errors"][:5]:
                print(f"  Error: {e}")
    else:
        print("\ncall_reports.csv: ⚠️ NOT FOUND")


if __name__ == "__main__":
    main()
