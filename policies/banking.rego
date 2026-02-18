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

region_filter := input.user.region if {
    input.user.region
    input.user.region != "all"
}

region_constraints := {"region_filter": region_filter} if {
    region_filter
}

region_constraints := {} if {
    not region_filter
}

purpose_constraints := {"must_aggregate_to_month": true} if {
    input.purpose == "reporting"
}

purpose_constraints := {"must_aggregate_to_quarter": true} if {
    input.purpose == "regulatory"
}

purpose_constraints := {"must_log_access": true, "forbid_export": true, "max_rows": 200} if {
    input.purpose == "investigation"
}

purpose_constraints := {} if {
    input.purpose == "analysis"
}

purpose_constraints := {} if {
    not input.purpose
}

min_group_override := {"min_group_size": input.policy_overrides.min_group_size} if {
    input.policy_overrides.min_group_size
} else := {}

forbid_export_override := {"forbid_export": true} if {
    input.policy_overrides.force_forbid_export
} else := {}

mask_override := {"must_mask": true} if {
    input.policy_overrides.force_mask
} else := {}

redact_override := {"must_redact_narratives": true} if {
    input.policy_overrides.force_redact
} else := {}

max_rows_override := {"max_rows": input.policy_overrides.max_rows} if {
    input.policy_overrides.max_rows
} else := {}

override_constraints := merge_constraints(
    merge_constraints(
        merge_constraints(
            merge_constraints(min_group_override, forbid_export_override),
            mask_override
        ),
        redact_override
    ),
    max_rows_override
)

override_region_constraints := {"region_filter": input.policy_overrides.region} if {
    input.policy_overrides.force_region_match
    input.policy_overrides.region
    input.policy_overrides.region != "all"
} else := {}

base_constraints := {"min_group_size": min_group_size}

merge_constraints(a, b) := object.union(a, b)

branch_manager_all_region if {
    input.user.role == "branch_manager"
    input.user.region == "all"
}

# ALLOW for LOW sensitivity queries
decision := {"result": "ALLOW", "reason": "Low sensitivity data allowed for all authenticated roles", "constraints": merge_constraints(merge_constraints(merge_constraints(merge_constraints(base_constraints, purpose_constraints), override_constraints), region_constraints), override_region_constraints)} if {
    input.user.role
    role_config
    not has_high_sensitivity
    not has_med_sensitivity
    not branch_manager_all_region
}

# ALLOW for MED sensitivity with sufficient role
decision := {"result": "ALLOW", "reason": "Medium sensitivity data allowed for role", "constraints": merge_constraints(merge_constraints(merge_constraints(merge_constraints(base_constraints, purpose_constraints), override_constraints), region_constraints), override_region_constraints)} if {
    input.user.role
    role_config
    has_med_sensitivity
    not has_high_sensitivity
    sensitivity_allowed("MED")
    not branch_manager_all_region
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
decision := {"result": "ALLOW_WITH_CONSTRAINTS", "reason": "High sensitivity data allowed with masking and logging for compliance officer", "constraints": merge_constraints(merge_constraints(merge_constraints(merge_constraints({"min_group_size": min_group_size, "must_mask": true, "must_log_access": true, "must_redact_narratives": true, "max_rows": 100, "forbid_export": false}, purpose_constraints), override_constraints), region_constraints), override_region_constraints)} if {
    input.user.role == "compliance_officer"
    has_high_sensitivity
    not branch_manager_all_region
}

# ALLOW_WITH_CONSTRAINTS for HIGH sensitivity (auditor)
decision := {"result": "ALLOW_WITH_CONSTRAINTS", "reason": "High sensitivity data allowed with constraints for auditor", "constraints": merge_constraints(merge_constraints(merge_constraints(merge_constraints({"min_group_size": min_group_size, "must_mask": true, "must_log_access": true, "must_redact_narratives": true, "max_rows": 50, "forbid_export": true}, purpose_constraints), override_constraints), region_constraints), override_region_constraints)} if {
    input.user.role == "auditor"
    has_high_sensitivity
    not branch_manager_all_region
}


decision := {"result": "DENY", "reason": "Branch manager must select a specific region", "constraints": {}} if {
    input.user.role == "branch_manager"
    input.user.region == "all"
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
