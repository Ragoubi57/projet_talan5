"""Ingestion script - generates data if missing, validates schema, seeds DuckDB."""
import os
import sys
import subprocess

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")


def main():
    print("=== Data Ingestion Pipeline ===\n")

    # Step 1: Generate synthetic data if not present
    complaints_path = os.path.join(DATA_DIR, "complaints.csv")
    call_reports_path = os.path.join(DATA_DIR, "call_reports.csv")

    if not os.path.exists(complaints_path) or not os.path.exists(call_reports_path):
        print("Step 1: Generating synthetic data...")
        subprocess.run([sys.executable, os.path.join(PROJECT_ROOT, "scripts", "generate_synth_data.py")], check=True)
    else:
        print("Step 1: Data files already exist, skipping generation.")

    # Step 2: Validate schema
    print("\nStep 2: Validating schemas...")
    subprocess.run([sys.executable, os.path.join(PROJECT_ROOT, "scripts", "validate_schema.py")], check=True)

    # Step 3: Seed DuckDB
    print("\nStep 3: Seeding DuckDB...")
    subprocess.run([sys.executable, os.path.join(PROJECT_ROOT, "scripts", "seed_duckdb.py")], check=True)

    # Step 4: Promote data products
    print("\nStep 4: Promoting data products...")
    subprocess.run([sys.executable, os.path.join(PROJECT_ROOT, "scripts", "promote.py")], check=True)

    print("\n=== Ingestion Complete ===")


if __name__ == "__main__":
    main()
