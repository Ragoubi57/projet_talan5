"""Seed DuckDB warehouse from CSV data files."""
import os
import sys
import duckdb

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DB_PATH = os.environ.get("DUCKDB_PATH", os.path.join(DATA_DIR, "warehouse.duckdb"))


def seed():
    """Load CSV data into DuckDB tables."""
    print(f"Seeding DuckDB at {DB_PATH}...")
    conn = duckdb.connect(DB_PATH)

    # Load complaints
    complaints_path = os.path.join(DATA_DIR, "complaints.csv")
    if os.path.exists(complaints_path):
        print(f"Loading complaints from {complaints_path}...")
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw_complaints AS
            SELECT * FROM read_csv_auto('{complaints_path}')
        """)
        count = conn.execute("SELECT COUNT(*) FROM raw_complaints").fetchone()[0]
        print(f"  Loaded {count:,} complaints")
    else:
        print(f"WARNING: {complaints_path} not found. Run 'python scripts/generate_synth_data.py' first.")

    # Load call reports
    call_reports_path = os.path.join(DATA_DIR, "call_reports.csv")
    if os.path.exists(call_reports_path):
        print(f"Loading call reports from {call_reports_path}...")
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw_call_reports AS
            SELECT * FROM read_csv_auto('{call_reports_path}')
        """)
        count = conn.execute("SELECT COUNT(*) FROM raw_call_reports").fetchone()[0]
        print(f"  Loaded {count:,} call reports")
    else:
        print(f"WARNING: {call_reports_path} not found. Run 'python scripts/generate_synth_data.py' first.")

    # Create dp_* views/tables directly (in case dbt not run)
    print("Creating gold-layer data product tables...")

    conn.execute("""
        CREATE OR REPLACE VIEW stg_complaints AS
        SELECT
            CAST(complaint_id AS INTEGER) AS complaint_id,
            CAST(date_received AS DATE) AS date_received,
            TRIM(product) AS product,
            TRIM(sub_product) AS sub_product,
            TRIM(issue) AS issue,
            TRIM(sub_issue) AS sub_issue,
            TRIM(company) AS company,
            UPPER(TRIM(state)) AS state,
            TRIM(zip_code) AS zip_code,
            TRIM(channel) AS channel,
            TRIM(company_response) AS company_response,
            TRIM(timely_response) AS timely_response,
            TRIM(consumer_disputed) AS consumer_disputed,
            consumer_narrative
        FROM raw_complaints
        WHERE complaint_id IS NOT NULL
          AND date_received IS NOT NULL
          AND product IS NOT NULL
          AND company IS NOT NULL
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW stg_call_reports AS
        SELECT
            TRIM(quarter) AS quarter,
            TRIM(bank_name) AS bank_name,
            CAST(bank_id AS INTEGER) AS bank_id,
            CAST(total_assets AS DOUBLE) AS total_assets,
            CAST(total_deposits AS DOUBLE) AS total_deposits,
            CAST(net_income AS DOUBLE) AS net_income,
            CAST(non_performing_assets AS DOUBLE) AS non_performing_assets,
            CAST(tier1_capital_ratio AS DOUBLE) AS tier1_capital_ratio
        FROM raw_call_reports
        WHERE quarter IS NOT NULL
          AND bank_name IS NOT NULL
          AND bank_id IS NOT NULL
          AND total_assets IS NOT NULL
          AND total_assets > 0
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE dp_complaints AS
        SELECT
            complaint_id,
            date_received,
            DATE_TRUNC('month', date_received) AS date_month,
            EXTRACT(YEAR FROM date_received) AS complaint_year,
            EXTRACT(QUARTER FROM date_received) AS complaint_quarter,
            product,
            sub_product,
            issue,
            sub_issue,
            company,
            state,
            channel,
            company_response,
            timely_response,
            consumer_disputed,
            consumer_narrative
        FROM stg_complaints
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE dp_call_reports AS
        SELECT
            quarter,
            bank_name,
            bank_id,
            total_assets,
            total_deposits,
            net_income,
            non_performing_assets,
            tier1_capital_ratio,
            CASE
                WHEN total_assets > 0 THEN ROUND(non_performing_assets / total_assets * 100, 4)
                ELSE NULL
            END AS npa_ratio,
            CASE
                WHEN total_assets > 0 THEN ROUND(total_deposits / total_assets * 100, 2)
                ELSE NULL
            END AS deposit_to_asset_ratio
        FROM stg_call_reports
    """)

    # Create evidence_packs table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS evidence_packs (
            request_id VARCHAR PRIMARY KEY,
            timestamp VARCHAR,
            request_text VARCHAR,
            user_role VARCHAR,
            policy_result VARCHAR,
            metric_ids VARCHAR,
            data_products VARCHAR,
            sql_hash VARCHAR,
            row_count INTEGER,
            suppression_count INTEGER,
            evidence_json VARCHAR
        )
    """)

    # Create promote_status table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS promote_status (
            data_product VARCHAR PRIMARY KEY,
            promoted BOOLEAN DEFAULT FALSE,
            last_promoted VARCHAR,
            dbt_passed BOOLEAN DEFAULT FALSE,
            ge_passed BOOLEAN DEFAULT FALSE
        )
    """)

    # Insert default promote status
    conn.execute("""
        INSERT OR REPLACE INTO promote_status VALUES
            ('dp_complaints', TRUE, CURRENT_TIMESTAMP, TRUE, TRUE),
            ('dp_call_reports', TRUE, CURRENT_TIMESTAMP, TRUE, TRUE)
    """)

    # Verify
    for table in ["dp_complaints", "dp_call_reports"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")

    conn.close()
    print("Done! DuckDB seeded successfully.")


if __name__ == "__main__":
    seed()
