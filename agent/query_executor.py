"""DuckDB query execution."""
import os
import duckdb
import logging
from typing import Dict, Any, Optional, Tuple
import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("DUCKDB_PATH", os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "warehouse.duckdb"
))


def get_connection():
    """Get a DuckDB connection."""
    return duckdb.connect(DB_PATH)


def run_query(sql: str, conn=None) -> Tuple[pd.DataFrame, int]:
    """Execute a SQL query against DuckDB and return results.
    
    Returns (dataframe, row_count).
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    try:
        result = conn.execute(sql)
        df = result.fetchdf()
        row_count = len(df)
        return df, row_count
    finally:
        if close_conn:
            conn.close()


def export_csv(df: pd.DataFrame, filename: str) -> str:
    """Export a DataFrame to CSV. Returns the file path."""
    export_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "artifacts", "exports"
    )
    os.makedirs(export_dir, exist_ok=True)
    filepath = os.path.join(export_dir, filename)
    df.to_csv(filepath, index=False)
    return filepath
