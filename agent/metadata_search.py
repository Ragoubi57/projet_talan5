"""Metadata search across metrics catalog and data products."""
import os
import sys
from typing import Dict, Any, List, Optional

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from catalog.loader import load_metrics, load_data_products, get_metric, get_data_product


def metadata_search(query: str) -> Dict[str, Any]:
    """Search metrics and data products matching a natural language query.
    
    Returns matching metrics and data products with their metadata.
    """
    query_lower = query.lower()
    results = {
        "metrics": [],
        "data_products": [],
        "suggested_metric": None,
    }

    # Search metrics
    metrics = load_metrics()
    for m in metrics:
        score = _score_metric(m, query_lower)
        if score > 0:
            results["metrics"].append({**m, "_relevance_score": score})

    # Sort by relevance
    results["metrics"].sort(key=lambda x: x["_relevance_score"], reverse=True)

    # Search data products
    data_products = load_data_products()
    for dp in data_products:
        score = _score_data_product(dp, query_lower)
        if score > 0:
            results["data_products"].append({**dp, "_relevance_score": score})

    results["data_products"].sort(key=lambda x: x["_relevance_score"], reverse=True)

    # Set suggested metric
    if results["metrics"]:
        results["suggested_metric"] = results["metrics"][0]["metric_id"]

    return results


def _score_metric(metric: Dict, query: str) -> float:
    """Score a metric against a query."""
    score = 0.0
    name_lower = metric.get("name", "").lower()
    desc_lower = metric.get("description", "").lower()
    mid = metric.get("metric_id", "").lower()

    # Direct metric_id match
    if mid in query:
        score += 10

    # Name match
    for word in query.split():
        if len(word) > 2:
            if word in name_lower:
                score += 5
            if word in desc_lower:
                score += 2
            if word in mid:
                score += 3

    # Keyword matching
    keyword_map = {
        "complaint": ["complaint_count"],
        "complaints": ["complaint_count"],
        "income": ["net_income_sum", "net_income_avg"],
        "net income": ["net_income_sum", "net_income_avg"],
        "deposit": ["deposits_sum"],
        "deposits": ["deposits_sum"],
        "tier1": ["tier1_ratio_avg"],
        "tier 1": ["tier1_ratio_avg"],
        "capital": ["tier1_ratio_avg"],
        "npa": ["npa_ratio"],
        "non-performing": ["npa_ratio"],
        "nonperforming": ["npa_ratio"],
    }
    for keyword, metric_ids in keyword_map.items():
        if keyword in query and mid in metric_ids:
            score += 8

    return score


def _score_data_product(dp: Dict, query: str) -> float:
    """Score a data product against a query."""
    score = 0.0
    name_lower = dp.get("name", "").lower()
    desc_lower = dp.get("description", "").lower()
    dp_id = dp.get("id", "").lower()

    for word in query.split():
        if len(word) > 2:
            if word in name_lower:
                score += 5
            if word in desc_lower:
                score += 2
            if word in dp_id:
                score += 3

    # Keyword matching
    if "complaint" in query or "complaints" in query:
        if "complaint" in dp_id:
            score += 10
    if "bank" in query or "financial" in query or "income" in query:
        if "call_report" in dp_id:
            score += 10
    if "deposit" in query or "asset" in query or "tier" in query:
        if "call_report" in dp_id:
            score += 8

    return score


def get_metric_details(metric_id: str) -> Optional[Dict[str, Any]]:
    """Get full details for a specific metric."""
    return get_metric(metric_id)


def get_data_product_details(dp_id: str) -> Optional[Dict[str, Any]]:
    """Get full details for a specific data product."""
    return get_data_product(dp_id)
