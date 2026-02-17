package banking

import rego.v1

default decision := {"result": "DENY", "reason": "No matching policy rule", "constraints": {}}

# Role definitions
roles := {
    "branch_manager": {"level": 1, "can_export": false, "max_sensitivity": "LOW"},
    "risk_officer": {"level": 2, "can_export": true, "max_sensitivity": "MED"},
    "compliance_officer": {"level": 3, "can_export": true, "max_sensitivity": "HIGH"},
    "auditor": {"level": 4, "can_export": true, "max_sensitivity": "HIGH"},
    "data_analyst": {"level": 1, "can_export": false, "max_sensitivity": "LOW"}
}

sensitivity_order := {"LOW": 1, "MED": 2, "HIGH": 3}

# Get the role config
role_config := roles[input.user.role]

# Check if sensitivity is allowed for role
sensitivity_allowed(col_sensitivity) if {
    role_level := sensitivity_order[roles[input.user.role].max_sensitivity]
    col_level := sensitivity_order[col_sensitivity]
    col_level <= role_level
}

# Check for HIGH sensitivity columns in request
has_high_sensitivity if {
    some col in input.columns
    col.sensitivity == "HIGH"
}

# Check for MED sensitivity columns
has_med_sensitivity if {
    some col in input.columns
    col.sensitivity == "MED"
}

# Min group size - default 10
min_group_size := input.policy_overrides.min_group_size if {
    input.policy_overrides.min_group_size
} else := 10

# ALLOW for LOW sensitivity queries
decision := {"result": "ALLOW", "reason": "Low sensitivity data allowed for all authenticated roles", "constraints": {"min_group_size": min_group_size}} if {
    input.user.role
    role_config
    not has_high_sensitivity
    not has_med_sensitivity
}

# ALLOW for MED sensitivity with sufficient role
decision := {"result": "ALLOW", "reason": "Medium sensitivity data allowed for role", "constraints": {"min_group_size": min_group_size}} if {
    input.user.role
    role_config
    has_med_sensitivity
    not has_high_sensitivity
    sensitivity_allowed("MED")
}

# DENY MED sensitivity for insufficient role
decision := {"result": "DENY", "reason": "Role does not have access to medium sensitivity data", "constraints": {}} if {
    input.user.role
    role_config
    has_med_sensitivity
    not has_high_sensitivity
    not sensitivity_allowed("MED")
}

# ALLOW_WITH_CONSTRAINTS for HIGH sensitivity (compliance_officer only)
decision := {"result": "ALLOW_WITH_CONSTRAINTS", "reason": "High sensitivity data allowed with masking and logging for compliance officer", "constraints": {"min_group_size": min_group_size, "must_mask": true, "must_log_access": true, "must_redact_narratives": true, "max_rows": 100, "forbid_export": false}} if {
    input.user.role == "compliance_officer"
    has_high_sensitivity
}

# ALLOW_WITH_CONSTRAINTS for HIGH sensitivity (auditor)
decision := {"result": "ALLOW_WITH_CONSTRAINTS", "reason": "High sensitivity data allowed with constraints for auditor", "constraints": {"min_group_size": min_group_size, "must_mask": true, "must_log_access": true, "must_redact_narratives": true, "max_rows": 50, "forbid_export": true}} if {
    input.user.role == "auditor"
    has_high_sensitivity
}

# DENY HIGH sensitivity for all other roles
decision := {"result": "DENY", "reason": "High sensitivity data (e.g., narratives) denied for this role. Consider requesting aggregated data instead.", "constraints": {}} if {
    input.user.role
    role_config
    has_high_sensitivity
    input.user.role != "compliance_officer"
    input.user.role != "auditor"
}

# Export decision
export_allowed if {
    role_config.can_export
}

# Purpose-based constraints
purpose_constraints := {"must_aggregate_to_month": true} if {
    input.purpose == "reporting"
}

purpose_constraints := {"must_aggregate_to_quarter": true} if {
    input.purpose == "regulatory"
}

purpose_constraints := {} if {
    not input.purpose
}
