"""Verifiable Banking Analytics - Streamlit UI"""
import os
import sys
import json
import pandas as pd
import altair as alt
import streamlit as st

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from agent.graph import process_request
from agent.policy_client import check_export_allowed

st.set_page_config(
    page_title="Verifiable Banking Analytics",
    page_icon="BA",
    layout="wide",
)

st.title("Verifiable Banking Analytics")
st.markdown("Ask analytics questions in natural language. Every response includes an auditable evidence pack.")

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "policy_matrix_results" not in st.session_state:
    st.session_state.policy_matrix_results = []


def _scenario_overrides(scenario: str, user: dict) -> dict:
    if scenario == "Standard (Default)":
        return {}
    if scenario == "Strict K-Anon (min 25)":
        return {"min_group_size": 25}
    if scenario == "Export Locked":
        return {"force_forbid_export": True}
    if scenario == "Mask + Redact":
        return {"force_mask": True, "force_redact": True}
    if scenario == "Regional Lockdown":
        return {"force_region_match": True, "region": user.get("region")}
    if scenario == "Max Rows 25":
        return {"max_rows": 25}
    return {}


def _run_request(q: str, user: dict, mode: str, scenario: str) -> None:
    overrides = _scenario_overrides(scenario, user)
    with st.spinner("Processing your request through the verifiable analytics pipeline..."):
        result = process_request(
            q,
            user,
            policy_only=(mode == "Policy Test"),
            policy_overrides=overrides,
        )
    st.session_state.messages.append(
        {
            "query": q,
            "result": result,
            "user": user.copy(),
            "mode": mode,
            "scenario": scenario,
        }
    )


# Sidebar: profile and history
st.sidebar.header("User Profile")
mode = st.sidebar.radio("Mode", ["Query", "Policy Test"], index=0)
role = st.sidebar.selectbox(
    "Role",
    ["branch_manager", "risk_officer", "compliance_officer", "auditor", "data_analyst"],
    index=0,
)
region = st.sidebar.selectbox(
    "Region",
    ["northeast", "southeast", "midwest", "west", "all"],
    index=4,
)
purpose = st.sidebar.selectbox(
    "Purpose",
    ["reporting", "analysis", "investigation", "regulatory"],
    index=0,
)
scenario = st.sidebar.selectbox(
    "Policy Scenario",
    [
        "Standard (Default)",
        "Strict K-Anon (min 25)",
        "Export Locked",
        "Mask + Redact",
        "Regional Lockdown",
        "Max Rows 25",
    ],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.subheader("History")
if not st.session_state.messages:
    st.sidebar.caption("No queries yet.")
else:
    for i, msg in enumerate(reversed(st.session_state.messages[-20:])):
        label = msg["query"]
        if len(label) > 42:
            label = label[:42] + "..."
        if st.sidebar.button(label, key=f"hist_{i}"):
            _run_request(msg["query"], msg["user"], msg.get("mode", mode), msg.get("scenario", scenario))

user = {"role": role, "region": region, "purpose": purpose}

if role == "branch_manager" and region == "all":
    st.sidebar.warning("Branch managers must select a specific region. Queries will be denied until you choose one.")

st.markdown("---")

example_queries = [
    "Show complaint counts by product and state for the last 12 months",
    "What is the average net income by bank by quarter?",
    "Show total deposits by quarter",
    "Average tier 1 capital ratio by bank",
    "Show me complaint narratives for investigations",
    "Export complaint counts by product as CSV",
    "Show net income trend with outlier detection",
    "NPA ratio by bank by quarter",
    "Timely response rate by region and month",
    "Deposit to asset ratio by bank region",
]

with st.expander("Quick Queries", expanded=(mode == "Query")):
    cols = st.columns(2)
    for idx, eq in enumerate(example_queries):
        with cols[idx % 2]:
            if st.button(eq, key=f"ex_{idx}"):
                _run_request(eq, user, mode, scenario)

prompt = st.chat_input("Ask an analytics question")
if prompt:
    _run_request(prompt, user, mode, scenario)

# Policy matrix
if mode == "Policy Test":
    st.subheader("Policy Test Matrix")
    matrix_queries_text = st.text_area(
        "Queries (one per line)",
        value="\n".join(example_queries[:3]),
        height=120,
    )
    m1, m2, m3 = st.columns(3)
    with m1:
        matrix_roles = st.multiselect(
            "Roles",
            ["branch_manager", "risk_officer", "compliance_officer", "auditor", "data_analyst"],
            default=["branch_manager", "risk_officer", "compliance_officer"],
        )
    with m2:
        matrix_regions = st.multiselect(
            "Regions",
            ["northeast", "southeast", "midwest", "west", "all"],
            default=["all"],
        )
    with m3:
        matrix_purposes = st.multiselect(
            "Purposes",
            ["reporting", "analysis", "investigation", "regulatory"],
            default=["analysis"],
        )
    allow_large = st.checkbox("Allow large matrix runs", value=False)
    run_matrix = st.button("Run Policy Matrix")

    if run_matrix:
        queries = [q.strip() for q in matrix_queries_text.splitlines() if q.strip()]
        total = len(queries) * len(matrix_roles) * len(matrix_regions) * len(matrix_purposes)
        if total == 0:
            st.warning("Provide at least one query, role, region, and purpose.")
        elif total > 200 and not allow_large:
            st.warning(f"Matrix size {total} is large. Enable 'Allow large matrix runs' to proceed.")
        else:
            results = []
            with st.spinner(f"Running policy matrix ({total} cases)..."):
                for q in queries:
                    for r in matrix_roles:
                        for reg in matrix_regions:
                            for purp in matrix_purposes:
                                u = {"role": r, "region": reg, "purpose": purp}
                                res = process_request(q, u, policy_only=True, policy_overrides=_scenario_overrides(scenario, u))
                                decision = res.get("policy_decision", {})
                                results.append({
                                    "query": q,
                                    "role": r,
                                    "region": reg,
                                    "purpose": purp,
                                    "decision": decision.get("result", "UNKNOWN"),
                                    "reason": decision.get("reason", ""),
                                    "constraints": json.dumps(decision.get("constraints", {})),
                                })
            st.session_state.policy_matrix_results = results

if mode == "Policy Test" and st.session_state.policy_matrix_results:
    st.subheader("Policy Matrix Results")
    matrix_df = pd.DataFrame(st.session_state.policy_matrix_results)
    st.dataframe(matrix_df, width="stretch")

# Chat history
for i, msg in enumerate(st.session_state.messages):
    query_text = msg["query"]
    result = msg["result"]
    msg_user = msg["user"]
    msg_mode = msg.get("mode", "Query")
    msg_scenario = msg.get("scenario", "Standard (Default)")

    with st.chat_message("user"):
        st.markdown(query_text)
        st.caption(f"role={msg_user['role']} | region={msg_user['region']} | purpose={msg_user['purpose']} | mode={msg_mode} | scenario={msg_scenario}")

    with st.chat_message("assistant"):
        if not result.get("success"):
            st.error(f"{result.get('error', 'Unknown error')}")
            if result.get("policy_decision"):
                with st.expander("Policy Decision"):
                    st.json(result["policy_decision"])
        else:
            st.markdown(result.get("explanation", ""))

            if result.get("results"):
                df = pd.DataFrame(result["results"])
                st.dataframe(df, width="stretch")

                chart_spec = result.get("chart_spec")
                if chart_spec and not df.empty:
                    try:
                        chart = alt.Chart(df).mark_bar() if chart_spec.get("mark") == "bar" else alt.Chart(df).mark_line(point=True)
                        x_enc = chart_spec["encoding"]["x"]
                        y_enc = chart_spec["encoding"]["y"]
                        chart = chart.encode(
                            x=alt.X(f"{x_enc['field']}:N" if x_enc.get("type") == "nominal" else f"{x_enc['field']}:O", sort=None),
                            y=alt.Y(f"{y_enc['field']}:Q"),
                        )
                        if "color" in chart_spec.get("encoding", {}):
                            color_field = chart_spec["encoding"]["color"]["field"]
                            chart = chart.encode(color=f"{color_field}:N")
                        chart = chart.properties(width=700, height=400)
                        st.altair_chart(chart, width="stretch")
                    except Exception as e:
                        st.warning(f"Chart rendering error: {e}")

                if result.get("export_path"):
                    st.success(f"CSV exported to: {result['export_path']}")
                elif check_export_allowed(msg_user["role"]):
                    csv_data = df.to_csv(index=False)
                    st.download_button(
                        "Export CSV",
                        csv_data,
                        file_name="analytics_export.csv",
                        mime="text/csv",
                        key=f"export_{i}",
                    )
                else:
                    st.info("Export not available for your role.")

            with st.expander("Evidence Pack"):
                if result.get("evidence_pack"):
                    st.json(result["evidence_pack"])
                    ep_json = json.dumps(result["evidence_pack"], indent=2, default=str)
                    st.download_button(
                        "Download Evidence Pack",
                        ep_json,
                        file_name=f"evidence_pack_{result['evidence_pack'].get('request_id', 'unknown')}.json",
                        mime="application/json",
                        key=f"ep_download_{i}",
                    )

            with st.expander("Details"):
                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown("**Policy Decision:**")
                    st.json(result.get("policy_decision", {}))
                with col_b:
                    st.markdown("**Quality Status:**")
                    st.json(result.get("quality_info", {}))

                st.markdown(f"**SQL Hash:** `{result.get('sql_hash', '')}`")
                st.code(result.get("sql", ""), language="sql")
