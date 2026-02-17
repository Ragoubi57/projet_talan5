"""Catalog loader for metrics and data products."""
import os
import json
import yaml
from typing import Dict, List, Optional, Any

CATALOG_DIR = os.path.dirname(os.path.abspath(__file__))

def load_metrics() -> List[Dict[str, Any]]:
    path = os.path.join(CATALOG_DIR, "metrics.yml")
    with open(path) as f:
        return yaml.safe_load(f).get("metrics", [])

def load_data_products() -> List[Dict[str, Any]]:
    path = os.path.join(CATALOG_DIR, "data_products.yml")
    with open(path) as f:
        return yaml.safe_load(f).get("data_products", [])

def load_schema(name: str) -> Dict[str, Any]:
    path = os.path.join(CATALOG_DIR, "schemas", f"{name}_schema.json")
    with open(path) as f:
        return json.load(f)

def get_metric(metric_id: str) -> Optional[Dict[str, Any]]:
    for m in load_metrics():
        if m["metric_id"] == metric_id:
            return m
    return None

def get_data_product(dp_id: str) -> Optional[Dict[str, Any]]:
    for dp in load_data_products():
        if dp["id"] == dp_id:
            return dp
    return None

def get_sensitive_columns(dp_id: str) -> List[str]:
    dp = get_data_product(dp_id)
    if not dp:
        return []
    return [c["name"] for c in dp.get("columns", []) if c.get("sensitivity") == "HIGH"]

def get_column_sensitivity(dp_id: str, column_name: str) -> str:
    dp = get_data_product(dp_id)
    if not dp:
        return "HIGH"  # default to most restrictive
    for c in dp.get("columns", []):
        if c["name"] == column_name:
            return c.get("sensitivity", "HIGH")
    return "HIGH"

def get_allowed_data_products() -> List[str]:
    return [dp["id"] for dp in load_data_products()]
