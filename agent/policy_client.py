"""Policy evaluation client - calls OPA REST API or uses local fallback."""
import json
import os
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

OPA_URL = os.environ.get("OPA_URL", "http://localhost:8181")

# Local fallback policy for when OPA is not available
ROLE_CONFIGS = {
    "branch_manager": {"level": 1, "can_export": False, "max_sensitivity": "LOW"},
    "risk_officer": {"level": 2, "can_export": True, "max_sensitivity": "MED"},
    "compliance_officer": {"level": 3, "can_export": True, "max_sensitivity": "HIGH"},
    "auditor": {"level": 4, "can_export": True, "max_sensitivity": "HIGH"},
    "data_analyst": {"level": 1, "can_export": False, "max_sensitivity": "LOW"},
}

SENSITIVITY_ORDER = {"LOW": 1, "MED": 2, "HIGH": 3}


def policy_eval(request: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate policy. Tries OPA first, falls back to local logic."""
    try:
        return _call_opa(request)
    except Exception as e:
        logger.warning(f"OPA unavailable ({e}), using local policy fallback")
        return _local_policy_eval(request)


def _call_opa(request: Dict[str, Any]) -> Dict[str, Any]:
    """Call OPA REST API."""
    import urllib.request
    url = f"{OPA_URL}/v1/data/banking/decision"
    data = json.dumps({"input": request}).encode('utf-8')
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        result = json.loads(resp.read().decode())
        return result.get("result", {"result": "DENY", "reason": "No result from OPA", "constraints": {}})


def _local_policy_eval(request: Dict[str, Any]) -> Dict[str, Any]:
    """Local policy evaluation fallback."""
    user = request.get("user", {})
    role = user.get("role", "")
    columns = request.get("columns", [])
    overrides = request.get("policy_overrides", {}) or {}

    role_config = ROLE_CONFIGS.get(role)
    if not role_config:
        return {"result": "DENY", "reason": f"Unknown role: {role}", "constraints": {}}

    max_sens = role_config["max_sensitivity"]
    max_level = SENSITIVITY_ORDER.get(max_sens, 0)

    has_high = any(c.get("sensitivity") == "HIGH" for c in columns)
    has_med = any(c.get("sensitivity") == "MED" for c in columns)
    min_group_size = overrides.get("min_group_size", 10)
    region = user.get("region")
    purpose = user.get("purpose")

    if role == "branch_manager" and region == "all":
        return {"result": "DENY", "reason": "Branch manager must select a specific region", "constraints": {}}

    if has_high:
        if role in ("compliance_officer", "auditor"):
            max_rows = 100 if role == "compliance_officer" else 50
            decision = {
                "result": "ALLOW_WITH_CONSTRAINTS",
                "reason": f"High sensitivity data allowed with masking for {role}",
                "constraints": {
                    "min_group_size": min_group_size,
                    "must_mask": True,
                    "must_log_access": True,
                    "must_redact_narratives": True,
                    "max_rows": max_rows,
                    "forbid_export": role == "auditor"
                }
            }
            decision["constraints"] = _apply_purpose_constraints(decision["constraints"], purpose)
            decision["constraints"] = _apply_override_constraints(decision["constraints"], overrides)
            if region and region != "all":
                decision["constraints"]["region_filter"] = region
            return decision
        decision = {
            "result": "DENY",
            "reason": "High sensitivity data denied for this role. Consider requesting aggregated data instead.",
            "constraints": {}
        }
        return decision

    if has_med and SENSITIVITY_ORDER.get("MED", 2) > max_level:
        decision = {
            "result": "DENY",
            "reason": "Role does not have access to medium sensitivity data",
            "constraints": {}
        }
        return decision

    decision = {
        "result": "ALLOW",
        "reason": "Query allowed for role",
        "constraints": {"min_group_size": min_group_size}
    }
    decision["constraints"] = _apply_purpose_constraints(decision["constraints"], purpose)
    decision["constraints"] = _apply_override_constraints(decision["constraints"], overrides)
    if region and region != "all":
        decision["constraints"]["region_filter"] = region
    return decision


def check_export_allowed(role: str) -> bool:
    """Check if export is allowed for a role."""
    config = ROLE_CONFIGS.get(role, {})
    return config.get("can_export", False)


def _apply_purpose_constraints(constraints: Dict[str, Any], purpose: Optional[str]) -> Dict[str, Any]:
    merged = dict(constraints)
    if purpose == "reporting":
        merged["must_aggregate_to_month"] = True
    if purpose == "regulatory":
        merged["must_aggregate_to_quarter"] = True
    if purpose == "investigation":
        merged["must_log_access"] = True
        merged["forbid_export"] = True
        merged["max_rows"] = min(int(merged.get("max_rows", 200)), 200)
    return merged


def _apply_override_constraints(constraints: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(constraints)
    if "min_group_size" in overrides:
        merged["min_group_size"] = overrides["min_group_size"]
    if overrides.get("force_forbid_export"):
        merged["forbid_export"] = True
    if overrides.get("force_mask"):
        merged["must_mask"] = True
    if overrides.get("force_redact"):
        merged["must_redact_narratives"] = True
    if "max_rows" in overrides:
        merged["max_rows"] = int(overrides["max_rows"])
    if overrides.get("force_region_match"):
        region = overrides.get("region")
        if region:
            merged["region_filter"] = region
    return merged
