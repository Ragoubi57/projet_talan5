"""OpenLineage event recording - sends to Marquez or logs locally."""
import json
import os
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

MARQUEZ_URL = os.environ.get("MARQUEZ_URL", "http://localhost:5000")
LINEAGE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "artifacts", "lineage_events")
os.makedirs(LINEAGE_DIR, exist_ok=True)


def lineage_record(
    job_name: str,
    inputs: List[str],
    outputs: List[str],
    sql: str,
    user: str,
    request_id: str,
) -> str:
    """Record a lineage event. Returns event_id."""
    event_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()

    event = {
        "eventType": "COMPLETE",
        "eventTime": timestamp,
        "run": {
            "runId": event_id,
        },
        "job": {
            "namespace": "banking_analytics",
            "name": job_name,
        },
        "inputs": [
            {"namespace": "banking_analytics", "name": inp} for inp in inputs
        ],
        "outputs": [
            {"namespace": "banking_analytics", "name": out} for out in outputs
        ],
        "producer": "banking-analytics-agent",
        "metadata": {
            "sql": sql,
            "user": user,
            "request_id": request_id,
        },
    }

    # Save locally
    filepath = os.path.join(LINEAGE_DIR, f"{event_id}.json")
    with open(filepath, 'w') as f:
        json.dump(event, f, indent=2)

    # Try to send to Marquez
    try:
        _send_to_marquez(event)
    except Exception as e:
        logger.warning(f"Could not send lineage to Marquez: {e}")

    return event_id


def _send_to_marquez(event: Dict[str, Any]):
    """Send OpenLineage event to Marquez API."""
    import urllib.request
    url = f"{MARQUEZ_URL}/api/v1/lineage"
    data = json.dumps(event).encode('utf-8')
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        pass
