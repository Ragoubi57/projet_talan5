"""SQL validation and normalization using sqlglot."""
import hashlib
import re
from typing import Dict, Any, Tuple

ALLOWED_TABLES = {"dp_complaints", "dp_call_reports", "dp_macro_rates"}
FORBIDDEN_KEYWORDS = {"PRAGMA", "ATTACH", "DETACH", "COPY", "EXPORT", "IMPORT",
                      "CREATE", "DROP", "ALTER", "INSERT", "UPDATE", "DELETE",
                      "TRUNCATE", "GRANT", "REVOKE", "LOAD", "INSTALL"}

def validate_sql(sql: str) -> Tuple[bool, str]:
    """Validate SQL query for safety. Returns (is_valid, error_message)."""
    sql_upper = sql.upper().strip()
    
    # Check for forbidden keywords
    for kw in FORBIDDEN_KEYWORDS:
        pattern = r'\b' + kw + r'\b'
        if re.search(pattern, sql_upper):
            return False, f"Forbidden SQL keyword: {kw}"
    
    # Check for file operations
    if "READ_CSV" in sql_upper or "READ_PARQUET" in sql_upper or "READ_JSON" in sql_upper:
        return False, "Direct file reads are not allowed"
    
    # Extract table references - simple approach
    tables_referenced = extract_tables(sql)
    
    for table in tables_referenced:
        if table.lower() not in ALLOWED_TABLES and not table.lower().startswith("dp_"):
            return False, f"Table '{table}' is not an allowed data product. Only dp_* tables are queryable."
    
    if not tables_referenced:
        # Allow queries like SELECT 1
        pass
    
    return True, ""

def extract_tables(sql: str) -> set:
    """Extract table names from SQL using regex (lightweight approach)."""
    tables = set()
    # Match FROM and JOIN clauses
    patterns = [
        r'\bFROM\s+(\w+)',
        r'\bJOIN\s+(\w+)',
    ]
    for pattern in patterns:
        matches = re.findall(pattern, sql, re.IGNORECASE)
        tables.update(matches)
    return tables

def normalize_sql(sql: str) -> str:
    """Normalize SQL for consistent hashing."""
    # Remove extra whitespace
    normalized = re.sub(r'\s+', ' ', sql.strip())
    # Uppercase keywords
    normalized = normalized.upper()
    return normalized

def hash_sql(sql: str) -> str:
    """Compute SHA256 hash of normalized SQL."""
    canonical = normalize_sql(sql)
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()

def apply_min_group_size(sql: str, min_size: int = 10) -> str:
    """Add HAVING clause for min group size if query has GROUP BY."""
    sql_upper = sql.upper()
    if "GROUP BY" in sql_upper:
        # Check if HAVING already exists
        if "HAVING" in sql_upper:
            # Append to existing HAVING
            sql = re.sub(
                r'(HAVING\s+)',
                f'\\1COUNT(*) >= {min_size} AND ',
                sql,
                flags=re.IGNORECASE
            )
        else:
            # Add HAVING before ORDER BY or at end
            if "ORDER BY" in sql_upper:
                sql = re.sub(
                    r'(\s+ORDER\s+BY)',
                    f' HAVING COUNT(*) >= {min_size}\\1',
                    sql,
                    flags=re.IGNORECASE
                )
            elif "LIMIT" in sql_upper:
                sql = re.sub(
                    r'(\s+LIMIT)',
                    f' HAVING COUNT(*) >= {min_size}\\1',
                    sql,
                    flags=re.IGNORECASE
                )
            else:
                sql = sql.rstrip(';') + f' HAVING COUNT(*) >= {min_size}'
    return sql

def remove_forbidden_columns(sql: str, forbidden_columns: list) -> str:
    """Remove forbidden columns from SELECT. Very basic implementation."""
    for col in forbidden_columns:
        # Remove column from SELECT list
        sql = re.sub(rf',\s*{col}\b', '', sql, flags=re.IGNORECASE)
        sql = re.sub(rf'\b{col}\s*,', '', sql, flags=re.IGNORECASE)
        sql = re.sub(rf'\b{col}\b(?!\s*FROM)', '', sql, flags=re.IGNORECASE)
    return sql
