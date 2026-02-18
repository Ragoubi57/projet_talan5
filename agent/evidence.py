"""Evidence pack creation and storage."""
import json
import os
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

ARTIFACTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "artifacts", "evidence_packs")
os.makedirs(ARTIFACTS_DIR, exist_ok=True)


def make_evidence_pack(
    request_text: str,
    user_attrs: Dict[str, Any],
    policy_decision: Dict[str, Any],
    metric_ids: List[str],
    metric_versions: Dict[str, str],
    data_products_used: List[str],
    data_product_versions: Dict[str, str],
    freshness_status: Dict[str, Any],
    quality_status: Dict[str, Any],
    sql_text: str,
    canonical_sql: str,
    sql_hash: str,
    row_count: int,
    suppression_count: int = 0,
    lineage_event_id: Optional[str] = None,
    export_path: Optional[str] = None,
    db_conn=None,
) -> Dict[str, Any]:
    """Create and store an evidence pack."""
    request_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()

    evidence = {
        "request_id": request_id,
        "timestamp": timestamp,
        "request_text": request_text,
        "user_attributes": user_attrs,
        "policy_decision": {
            "result": policy_decision.get("result"),
            "reason": policy_decision.get("reason"),
            "constraints_applied": policy_decision.get("constraints", {}),
        },
        "metrics": {
            "metric_ids": metric_ids,
            "metric_versions": metric_versions,
        },
        "data_products": {
            "products_used": data_products_used,
            "product_versions": data_product_versions,
        },
        "data_quality": {
            "freshness": freshness_status,
            "quality_checks": quality_status,
        },
        "sql": {
            "final_sql": sql_text,
            "canonical_sql": canonical_sql,
            "sql_hash": sql_hash,
        },
        "results": {
            "row_count": row_count,
            "suppression_count": suppression_count,
        },
        "lineage": {
            "event_id": lineage_event_id,
        },
        "export": {
            "artifact_path": export_path,
        },
    }

    # Write to file
    filepath = os.path.join(ARTIFACTS_DIR, f"{request_id}.json")
    with open(filepath, 'w') as f:
        json.dump(evidence, f, indent=2, default=str)

    # Store in DuckDB if connection provided
    if db_conn is not None:
        try:
            _store_evidence_db(db_conn, evidence)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Failed to store evidence in DB: {e}")

    return evidence


def _store_evidence_db(conn, evidence: Dict[str, Any]):
    """Store evidence pack in DuckDB."""
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
    conn.execute("""
        INSERT OR REPLACE INTO evidence_packs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        evidence["request_id"],
        evidence["timestamp"],
        evidence["request_text"],
        evidence["user_attributes"].get("role", ""),
        evidence["policy_decision"]["result"],
        json.dumps(evidence["metrics"]["metric_ids"]),
        json.dumps(evidence["data_products"]["products_used"]),
        evidence["sql"]["sql_hash"],
        evidence["results"]["row_count"],
        evidence["results"]["suppression_count"],
        json.dumps(evidence, default=str),
    ])
