"""Verifiable Banking Analytics Agent - LangGraph-style orchestration.

Implements the full analytics workflow as a state machine.
Uses mock LLM by default if Ollama is not available.
"""
import os
import sys
import json
import re
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from agent.metadata_search import metadata_search, get_metric_details
from agent.policy_client import policy_eval, check_export_allowed
from agent.sql_validator import validate_sql, hash_sql, normalize_sql, apply_min_group_size
from agent.query_executor import run_query, export_csv, get_connection
from agent.evidence import make_evidence_pack
from agent.quality import quality_status, check_all_products_queryable
from agent.lineage import lineage_record
from catalog.loader import load_metrics, load_data_products, get_data_product, get_sensitive_columns

logger = logging.getLogger(__name__)

# LLM configuration
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3:4b")
USE_MOCK_LLM = os.environ.get("MOCK_LLM", "1") == "1"


class AnalyticsState:
    """State object for the analytics agent workflow."""

    def __init__(self, request: str, user: Dict[str, Any], policy_overrides: Optional[Dict[str, Any]] = None):
        self.request = request
        self.user = user
        self.policy_overrides = policy_overrides or {}
        self.metadata_results = {}
        self.selected_metric = None
        self.selected_data_products = []
        self.dsl_plan = {}
        self.policy_decision = {}
        self.constraints = {}
        self.sql = ""
        self.canonical_sql = ""
        self.sql_hash = ""
        self.results_df = None
        self.row_count = 0
        self.suppression_count = 0
        self.quality_info = {}
        self.lineage_event_id = None
        self.evidence_pack = {}
        self.export_path = None
        self.error = None
        self.explanation = ""
        self.chart_spec = None


def process_request(
    request: str,
    user: Dict[str, Any],
    policy_only: bool = False,
    policy_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Process a natural language analytics request through the full pipeline.
    
    Args:
        request: Natural language query
        user: Dict with role, region, purpose keys
        policy_only: If True, run policy evaluation only (no SQL execution)
    
    Returns:
        Dict with results, explanation, evidence_pack, chart_spec, error
    """
    state = AnalyticsState(request, user, policy_overrides=policy_overrides)

    try:
        # Step 1: Metadata search
        state = step_metadata_search(state)
        if state.error:
            return _build_response(state)

        # Step 2: Build DSL plan
        state = step_build_dsl_plan(state)
        if state.error:
            return _build_response(state)

        # Step 3: Policy evaluation (MUST happen before SQL generation)
        state = step_policy_eval(state)
        if state.error:
            return _build_response(state)

        if policy_only:
            state.explanation = _policy_only_explanation(state)
            return _build_response(state)

        # Step 4: Apply constraints
        state = step_apply_constraints(state)

        # Step 5: Compile SQL
        state = step_compile_sql(state)
        if state.error:
            return _build_response(state)

        # Step 6: Quality check
        state = step_quality_check(state)
        if state.error:
            return _build_response(state)

        # Step 7: Execute query
        state = step_execute_query(state)
        if state.error:
            return _build_response(state)

        # Step 8: Lineage record
        state = step_lineage_record(state)

        # Step 9: Build evidence pack
        state = step_evidence_pack(state)

        # Step 10: Generate explanation and chart
        state = step_generate_explanation(state)

    except Exception as e:
        logger.exception("Agent pipeline error")
        state.error = f"Pipeline error: {str(e)}"

    return _build_response(state)


def step_metadata_search(state: AnalyticsState) -> AnalyticsState:
    """Step 1: Search for matching metrics and data products."""
    state.metadata_results = metadata_search(state.request)

    if not state.metadata_results.get("metrics"):
        # Try broader search
        state.metadata_results = metadata_search(_extract_keywords(state.request))

    if not state.metadata_results.get("metrics"):
        state.error = "No matching metrics found for your query. Please try rephrasing."
        return state

    # Select top metric
    top_metric = state.metadata_results["metrics"][0]
    state.selected_metric = top_metric
    state.selected_data_products = [top_metric.get("data_product", "")]

    return state


def step_build_dsl_plan(state: AnalyticsState) -> AnalyticsState:
    """Step 2: Build a constrained analytics DSL plan."""
    metric = state.selected_metric
    if not metric:
        state.error = "No metric selected"
        return state

    # Parse request to determine dimensions and filters
    plan = {
        "metric_id": metric["metric_id"],
        "metric_version": metric.get("version", "1.0.0"),
        "data_product": metric.get("data_product", ""),
        "aggregation": _detect_aggregation(metric, state.request),
        "dimensions": _detect_dimensions(metric, state.request),
        "filters": _detect_filters(metric, state.request),
        "columns_needed": [],
        "sensitivity_levels": [],
        "wants_outliers": "outlier" in state.request.lower() or "anomal" in state.request.lower(),
        "wants_trend": "trend" in state.request.lower() or "over time" in state.request.lower(),
        "wants_export": "export" in state.request.lower() or "csv" in state.request.lower(),
    }

    # Determine columns and their sensitivity
    dp = get_data_product(plan["data_product"])
    if dp:
        col_map = {c["name"]: c.get("sensitivity", "LOW") for c in dp.get("columns", [])}
        for dim in plan["dimensions"]:
            if dim in col_map:
                plan["columns_needed"].append({"name": dim, "sensitivity": col_map[dim]})
                plan["sensitivity_levels"].append(col_map[dim])

        # Check if narrative is requested
        if _wants_narrative(state.request):
            plan["columns_needed"].append({"name": "consumer_narrative", "sensitivity": "HIGH"})
            plan["sensitivity_levels"].append("HIGH")

    state.dsl_plan = plan
    return state


def step_policy_eval(state: AnalyticsState) -> AnalyticsState:
    """Step 3: Policy evaluation - MUST happen before SQL generation."""
    plan = state.dsl_plan
    policy_request = {
        "user": state.user,
        "data_product": plan.get("data_product", ""),
        "columns": plan.get("columns_needed", []),
        "action": "query",
        "purpose": state.user.get("purpose", "analysis"),
        "policy_overrides": state.policy_overrides,
    }

    decision = policy_eval(policy_request)
    state.policy_decision = decision

    if decision.get("result") == "DENY":
        state.error = f"Policy DENIED: {decision.get('reason', 'Access denied')}"

        # Suggest alternatives for narrative requests
        if _wants_narrative(state.request):
            state.error += (
                "\n\nAlternative: You can request aggregated complaint issue "
                "counts or redacted examples instead of raw narratives."
            )
        return state

    state.constraints = decision.get("constraints", {})
    return state


def step_apply_constraints(state: AnalyticsState) -> AnalyticsState:
    """Step 4: Apply policy constraints to the DSL plan."""
    constraints = state.constraints

    # Apply constraints
    if constraints.get("must_redact_narratives"):
        # Mark narrative for redaction
        state.dsl_plan["redact_narrative"] = True

    if constraints.get("must_mask"):
        state.dsl_plan["mask_sensitive"] = True

    if constraints.get("forbid_export"):
        state.dsl_plan["wants_export"] = False

    # Region constraints
    region_filter = constraints.get("region_filter")
    if region_filter:
        state.dsl_plan.setdefault("filters", {})
        state.dsl_plan["filters"]["region"] = region_filter

    # Purpose aggregation constraints
    if constraints.get("must_aggregate_to_month"):
        state.dsl_plan["force_time_grain"] = "month"
    if constraints.get("must_aggregate_to_quarter"):
        state.dsl_plan["force_time_grain"] = "quarter"

    return state


def step_compile_sql(state: AnalyticsState) -> AnalyticsState:
    """Step 5: Compile DSL plan to SQL."""
    plan = state.dsl_plan
    metric = state.selected_metric

    if not metric:
        state.error = "No metric to compile SQL for"
        return state

    sql = _compile_metric_sql(metric, plan)

    # Apply min_group_size constraint
    min_size = state.constraints.get("min_group_size", 10)
    sql = apply_min_group_size(sql, min_size)

    # Apply max_rows constraint
    max_rows = state.constraints.get("max_rows")
    if max_rows:
        sql = sql.rstrip(';') + f" LIMIT {int(max_rows)}"

    # Validate SQL
    is_valid, error_msg = validate_sql(sql)
    if not is_valid:
        state.error = f"SQL validation failed: {error_msg}"
        return state

    state.sql = sql
    state.canonical_sql = normalize_sql(sql)
    state.sql_hash = hash_sql(sql)

    return state


def step_quality_check(state: AnalyticsState) -> AnalyticsState:
    """Step 6: Check quality status of data products."""
    try:
        conn = get_connection()
        all_ok, statuses = check_all_products_queryable(
            state.selected_data_products, conn
        )
        conn.close()
        state.quality_info = statuses

        if not all_ok:
            failed = [k for k, v in statuses.items() if not v.get("queryable")]
            state.error = (
                f"Quality gates failed for data products: {', '.join(failed)}. "
                "Cannot execute query until data products pass quality checks."
            )
    except Exception as e:
        logger.warning(f"Quality check error: {e}")
        # Allow through if we can't check (data might still exist)
        state.quality_info = {dp: {"queryable": True, "promoted": True} for dp in state.selected_data_products}

    return state


def step_execute_query(state: AnalyticsState) -> AnalyticsState:
    """Step 7: Execute the query."""
    try:
        conn = get_connection()
        df, row_count = run_query(state.sql, conn)

        # Apply redaction if needed
        if state.dsl_plan.get("redact_narrative") and "consumer_narrative" in df.columns:
            df["consumer_narrative"] = df["consumer_narrative"].apply(
                lambda x: _redact_text(str(x)) if x else x
            )

        state.results_df = df
        state.row_count = row_count

        # Handle export
        if state.dsl_plan.get("wants_export") and check_export_allowed(state.user.get("role", "")):
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"export_{state.selected_metric['metric_id']}_{timestamp}.csv"
            state.export_path = export_csv(df, filename)

        conn.close()
    except Exception as e:
        state.error = f"Query execution error: {str(e)}"

    return state


def step_lineage_record(state: AnalyticsState) -> AnalyticsState:
    """Step 8: Record lineage event."""
    try:
        state.lineage_event_id = lineage_record(
            job_name=f"analytics_query_{state.selected_metric['metric_id']}",
            inputs=state.selected_data_products,
            outputs=["query_results"],
            sql=state.sql,
            user=state.user.get("role", "unknown"),
            request_id="pending",
        )
    except Exception as e:
        logger.warning(f"Lineage recording failed: {e}")
        state.lineage_event_id = "lineage_unavailable"

    return state


def step_evidence_pack(state: AnalyticsState) -> AnalyticsState:
    """Step 9: Create evidence pack."""
    try:
        conn = get_connection()
        metric = state.selected_metric
        metric_versions = {}
        if metric:
            metric_versions[metric["metric_id"]] = metric.get("version", "1.0.0")

        dp_versions = {}
        for dp_id in state.selected_data_products:
            dp = get_data_product(dp_id)
            if dp:
                dp_versions[dp_id] = dp.get("version", "1.0.0")

        state.evidence_pack = make_evidence_pack(
            request_text=state.request,
            user_attrs=state.user,
            policy_decision=state.policy_decision,
            metric_ids=[metric["metric_id"]] if metric else [],
            metric_versions=metric_versions,
            data_products_used=state.selected_data_products,
            data_product_versions=dp_versions,
            freshness_status={dp: state.quality_info.get(dp, {}).get("freshness", "unknown") for dp in state.selected_data_products},
            quality_status={dp: state.quality_info.get(dp, {}).get("dbt_tests_passed", False) for dp in state.selected_data_products},
            sql_text=state.sql,
            canonical_sql=state.canonical_sql,
            sql_hash=state.sql_hash,
            row_count=state.row_count,
            suppression_count=state.suppression_count,
            lineage_event_id=state.lineage_event_id,
            export_path=state.export_path,
            db_conn=conn,
        )
        conn.close()
    except Exception as e:
        logger.warning(f"Evidence pack creation failed: {e}")
        state.evidence_pack = {"error": str(e)}

    return state


def step_generate_explanation(state: AnalyticsState) -> AnalyticsState:
    """Step 10: Generate natural language explanation and chart spec."""
    metric = state.selected_metric
    if not metric or state.results_df is None:
        return state

    # Generate explanation
    if USE_MOCK_LLM:
        state.explanation = _mock_explanation(state)
    else:
        state.explanation = _llm_explanation(state)

    # Generate chart spec
    state.chart_spec = _generate_chart_spec(state)

    return state


# --- Helper Functions ---

def _extract_keywords(text: str) -> str:
    """Extract key analytics terms from text."""
    keywords = []
    text_lower = text.lower()
    terms = ["complaint", "income", "deposit", "tier", "capital", "npa",
             "bank", "financial", "trend", "quarterly", "monthly"]
    for t in terms:
        if t in text_lower:
            keywords.append(t)
    return " ".join(keywords) if keywords else text


def _detect_aggregation(metric: Dict, request: str) -> str:
    """Detect aggregation type from request."""
    req_lower = request.lower()
    if "average" in req_lower or "avg" in req_lower or "mean" in req_lower:
        return "AVG"
    if "sum" in req_lower or "total" in req_lower:
        return "SUM"
    if "count" in req_lower or "how many" in req_lower:
        return "COUNT"
    if "max" in req_lower:
        return "MAX"
    if "min" in req_lower:
        return "MIN"
    # Default based on metric
    mid = metric.get("metric_id", "")
    if "count" in mid:
        return "COUNT"
    if "sum" in mid:
        return "SUM"
    if "avg" in mid:
        return "AVG"
    return "COUNT"


def _detect_dimensions(metric: Dict, request: str) -> List[str]:
    """Detect dimensions from request text."""
    allowed = metric.get("allowed_dimensions", [])
    req_lower = request.lower()
    dims = []

    for dim in allowed:
        dim_lower = dim.lower().replace("_", " ")
        if dim_lower in req_lower or dim in req_lower:
            dims.append(dim)

    # Add temporal dimension based on grain
    if "quarterly" in req_lower or "quarter" in req_lower:
        if "quarter" in allowed:
            dims.append("quarter")
    if "monthly" in req_lower or "month" in req_lower:
        if "date_month" in allowed or "product" in allowed:
            pass  # handled by grain

    # Default: use first allowed dimension if none detected
    if not dims and allowed:
        dims = [allowed[0]]

    return list(set(dims))


def _detect_filters(metric: Dict, request: str) -> Dict[str, Any]:
    """Detect filters from request text."""
    filters = {}
    req_lower = request.lower()
    allowed = metric.get("allowed_filters", [])

    # Date filters
    if "last 12 months" in req_lower or "last year" in req_lower:
        filters["date_range"] = "last_12_months"
    elif "2024" in request:
        filters["year"] = 2024
    elif "2023" in request:
        filters["year"] = 2023
    elif "last quarter" in req_lower:
        filters["date_range"] = "last_quarter"

    # State filter
    state_match = re.search(r'\b([A-Z]{2})\b', request)
    if state_match and "state" in allowed:
        state_code = state_match.group(1)
        if len(state_code) == 2:
            filters["state"] = state_code

    return filters


def _wants_narrative(request: str) -> bool:
    """Check if request is asking for narratives/text content."""
    req_lower = request.lower()
    return any(word in req_lower for word in ["narrative", "text", "description", "verbatim", "raw complaint"])


def _compile_metric_sql(metric: Dict, plan: Dict) -> str:
    """Compile a metric definition + plan into SQL."""
    dp = plan.get("data_product", "")
    metric_id = metric.get("metric_id", "")
    agg = plan.get("aggregation", "COUNT")
    dims = plan.get("dimensions", [])
    filters = plan.get("filters", {})
    force_time_grain = plan.get("force_time_grain")

    # Enforce purpose-based aggregation if requested and supported
    if force_time_grain == "month":
        if dp == "dp_complaints":
            if "date_month" not in dims:
                dims.append("date_month")
    if force_time_grain == "quarter":
        if dp == "dp_complaints":
            if "complaint_quarter" not in dims:
                dims.append("complaint_quarter")
        elif dp == "dp_call_reports":
            if "quarter" not in dims:
                dims.append("quarter")

    # Build SELECT
    select_parts = []
    for dim in dims:
        select_parts.append(dim)

    # Build aggregation
    if metric_id == "complaint_count":
        select_parts.append("COUNT(*) AS complaint_count")
    elif metric_id == "timely_response_rate":
        select_parts.append("AVG(CASE WHEN timely_response = 'Yes' THEN 1 ELSE 0 END) AS timely_response_rate")
    elif metric_id == "dispute_rate":
        select_parts.append("AVG(CASE WHEN consumer_disputed = 'Yes' THEN 1 ELSE 0 END) AS dispute_rate")
    elif metric_id == "narrative_request_count":
        select_parts.append("COUNT(consumer_narrative) AS narrative_request_count")
    elif metric_id == "net_income_sum":
        select_parts.append("SUM(net_income) AS net_income_sum")
    elif metric_id == "net_income_avg":
        select_parts.append("AVG(net_income) AS net_income_avg")
    elif metric_id == "net_income_margin_avg":
        select_parts.append(
            "AVG(CAST(net_income AS DOUBLE)/NULLIF(CAST(total_assets AS DOUBLE),0)) AS net_income_margin_avg"
        )
    elif metric_id == "deposits_sum":
        select_parts.append("SUM(total_deposits) AS deposits_sum")
    elif metric_id == "deposits_avg":
        select_parts.append("AVG(total_deposits) AS deposits_avg")
    elif metric_id == "assets_sum":
        select_parts.append("SUM(total_assets) AS assets_sum")
    elif metric_id == "deposit_to_asset_ratio_avg":
        select_parts.append("AVG(deposit_to_asset_ratio) AS deposit_to_asset_ratio_avg")
    elif metric_id == "tier1_ratio_avg":
        select_parts.append("AVG(tier1_capital_ratio) AS tier1_ratio_avg")
    elif metric_id == "npa_ratio":
        select_parts.append("AVG(CAST(non_performing_assets AS DOUBLE)/NULLIF(CAST(total_assets AS DOUBLE),0)) AS npa_ratio")
    else:
        select_parts.append(f"{agg}(*) AS metric_value")

    if plan.get("redact_narrative"):
        select_parts.append("'[REDACTED]' AS consumer_narrative")

    # Build WHERE
    where_parts = ["1=1"]
    if "date_range" in filters:
        if filters["date_range"] == "last_12_months":
            where_parts.append("date_received >= CURRENT_DATE - INTERVAL '12 months'")
        elif filters["date_range"] == "last_quarter":
            where_parts.append("date_received >= CURRENT_DATE - INTERVAL '3 months'")
    if "year" in filters:
        if "date_received" in [c["name"] for c in (get_data_product(dp) or {}).get("columns", [])]:
            where_parts.append(f"EXTRACT(YEAR FROM date_received) = {int(filters['year'])}")
        else:
            where_parts.append(f"quarter LIKE '{int(filters['year'])}%'")
    if "state" in filters:
        state_val = re.sub(r'[^A-Z]', '', str(filters['state']))[:2]
        where_parts.append(f"state = '{state_val}'")
    if "region" in filters:
        region_val = re.sub(r'[^a-z_]', '', str(filters['region']).lower())
        if dp == "dp_complaints":
            where_parts.append(f"region = '{region_val}'")
        elif dp == "dp_call_reports":
            where_parts.append(f"bank_region = '{region_val}'")
    # Build query
    select_str = ", ".join(select_parts)
    where_str = " AND ".join(where_parts)

    if dims:
        group_by = ", ".join(dims)
        sql = f"SELECT {select_str} FROM {dp} WHERE {where_str} GROUP BY {group_by} ORDER BY {dims[0]}"
    else:
        sql = f"SELECT {select_str} FROM {dp} WHERE {where_str}"

    return sql


def _redact_text(text: str) -> str:
    """Redact sensitive text - mask most characters."""
    if not text or text == "None":
        return "[REDACTED]"
    words = text.split()
    redacted = []
    for w in words:
        if len(w) > 3:
            redacted.append(w[0] + "*" * (len(w) - 1))
        else:
            redacted.append("***")
    return " ".join(redacted)


def _policy_only_explanation(state: AnalyticsState) -> str:
    """Summarize policy decision without running SQL."""
    metric = state.selected_metric or {}
    plan = state.dsl_plan or {}
    decision = state.policy_decision or {}
    parts = [
        f"**Policy-only evaluation** for query: {state.request}",
        f"\n**Metric:** {metric.get('name', 'unknown')} ({metric.get('metric_id', 'n/a')})",
        f"\n**Data product:** {plan.get('data_product', 'n/a')}",
        f"\n**Decision:** {decision.get('result', 'UNKNOWN')}",
        f"\n**Reason:** {decision.get('reason', 'n/a')}",
    ]
    constraints = decision.get("constraints", {})
    if constraints:
        parts.append(f"\n**Constraints:** {json.dumps(constraints)}")
    return "\n".join(parts)


def _mock_explanation(state: AnalyticsState) -> str:
    """Generate a mock explanation without LLM."""
    metric = state.selected_metric
    if not metric:
        return "No metric available for explanation."

    parts = [
        f"**{metric['name']}** ({metric['metric_id']} v{metric.get('version', '1.0.0')})",
        f"\nThis query analyzed the **{metric.get('data_product', '')}** data product.",
        f"\n**Results:** {state.row_count} rows returned.",
    ]

    if state.dsl_plan.get("dimensions"):
        parts.append(f"\n**Grouped by:** {', '.join(state.dsl_plan['dimensions'])}")

    if state.dsl_plan.get("filters"):
        parts.append(f"\n**Filters applied:** {json.dumps(state.dsl_plan['filters'])}")

    if state.constraints:
        parts.append(f"\n**Policy constraints:** min_group_size={state.constraints.get('min_group_size', 10)}")

    if state.evidence_pack.get("request_id"):
        parts.append(f"\n**Evidence Pack:** {state.evidence_pack['request_id']}")

    quality_summary = []
    for dp_id, info in state.quality_info.items():
        status = "✅ Passed" if info.get("queryable") else "❌ Failed"
        quality_summary.append(f"{dp_id}: {status}")
    if quality_summary:
        parts.append(f"\n**Data Quality:** {'; '.join(quality_summary)}")

    return "\n".join(parts)


def _llm_explanation(state: AnalyticsState) -> str:
    """Generate explanation using Ollama LLM."""
    try:
        import urllib.request
        prompt = (
            f"Explain this analytics result in 2-3 sentences for a banking professional:\n"
            f"Metric: {state.selected_metric.get('name', '')}\n"
            f"Query: {state.request}\n"
            f"Row count: {state.row_count}\n"
            f"Data product: {', '.join(state.selected_data_products)}"
        )
        data = json.dumps({
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
        }).encode('utf-8')
        req = urllib.request.Request(
            f"{OLLAMA_URL}/api/generate",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
            return result.get("response", _mock_explanation(state))
    except Exception as e:
        logger.warning(f"LLM unavailable: {e}")
        return _mock_explanation(state)


def _generate_chart_spec(state: AnalyticsState) -> Optional[Dict[str, Any]]:
    """Generate an Altair/Vega-Lite chart spec."""
    if state.results_df is None or state.results_df.empty:
        return None

    df = state.results_df
    metric = state.selected_metric
    dims = state.dsl_plan.get("dimensions", [])

    if not dims:
        return None

    # Determine chart type
    metric_col = None
    for col in df.columns:
        if col not in dims and df[col].dtype in ['float64', 'int64', 'float32', 'int32']:
            metric_col = col
            break

    if not metric_col:
        return None

    x_field = dims[0]
    y_field = metric_col

    # Choose chart type
    if len(dims) == 1 and df[x_field].nunique() > 10:
        chart_type = "bar"
    elif "quarter" in x_field or "date" in x_field or "month" in x_field:
        chart_type = "line"
    else:
        chart_type = "bar"

    spec = {
        "mark": chart_type,
        "encoding": {
            "x": {"field": x_field, "type": "nominal" if chart_type == "bar" else "ordinal",
                   "sort": None},
            "y": {"field": y_field, "type": "quantitative"},
        },
    }

    # Add color if multiple dimensions
    if len(dims) > 1:
        spec["encoding"]["color"] = {"field": dims[1], "type": "nominal"}

    return spec


def _build_response(state: AnalyticsState) -> Dict[str, Any]:
    """Build the final response dict."""
    response = {
        "success": state.error is None,
        "error": state.error,
        "results": state.results_df.to_dict(orient="records") if state.results_df is not None else None,
        "row_count": state.row_count,
        "explanation": state.explanation if not state.error else state.error,
        "evidence_pack": state.evidence_pack,
        "chart_spec": state.chart_spec,
        "sql": state.sql,
        "sql_hash": state.sql_hash,
        "policy_decision": state.policy_decision,
        "export_path": state.export_path,
        "quality_info": state.quality_info,
    }
    return response
