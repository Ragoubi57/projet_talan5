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
    page_icon="🏦",
    layout="wide",
)

st.title("🏦 Verifiable Banking Analytics")
st.markdown("Ask analytics questions in natural language. Every response includes an auditable evidence pack.")

# Sidebar - User Configuration
st.sidebar.header("👤 User Profile")
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

user = {"role": role, "region": region, "purpose": purpose}

st.sidebar.markdown("---")
st.sidebar.markdown("### 📝 Example Queries")
example_queries = [
    "Show complaint counts by product and state for the last 12 months",
    "What is the average net income by bank by quarter?",
    "Show total deposits by quarter",
    "Average tier 1 capital ratio by bank",
    "Show me complaint narratives for investigations",
    "Export complaint counts by product as CSV",
    "Show net income trend with outlier detection",
    "NPA ratio by bank by quarter",
]
for eq in example_queries:
    if st.sidebar.button(eq, key=f"ex_{eq[:30]}"):
        st.session_state["query_input"] = eq

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "query_input" not in st.session_state:
    st.session_state.query_input = ""

# Main chat interface
query = st.text_input(
    "🔍 Ask an analytics question:",
    value=st.session_state.get("query_input", ""),
    key="main_query",
    placeholder="e.g., Show complaint counts by product for the last 12 months",
)

col1, col2 = st.columns([1, 5])
with col1:
    submit = st.button("🚀 Submit", type="primary")

if submit and query:
    st.session_state.query_input = ""

    with st.spinner("Processing your request through the verifiable analytics pipeline..."):
        result = process_request(query, user)

    st.session_state.messages.append({"query": query, "result": result, "user": user.copy()})

# Display results
for i, msg in enumerate(reversed(st.session_state.messages)):
    result = msg["result"]
    query_text = msg["query"]
    msg_user = msg["user"]

    with st.container():
        st.markdown(f"### 💬 Query: {query_text}")
        st.markdown(f"*Role: {msg_user['role']} | Region: {msg_user['region']} | Purpose: {msg_user['purpose']}*")

        if not result.get("success"):
            st.error(f"❌ {result.get('error', 'Unknown error')}")
            # Show policy decision if available
            if result.get("policy_decision"):
                with st.expander("📋 Policy Decision"):
                    st.json(result["policy_decision"])
        else:
            # Results panel
            tab1, tab2, tab3 = st.tabs(["📊 Results", "📋 Evidence Pack", "🔍 Details"])

            with tab1:
                # Explanation
                st.markdown(result.get("explanation", ""))

                # Table
                if result.get("results"):
                    df = pd.DataFrame(result["results"])
                    st.dataframe(df, use_container_width=True)

                    # Chart
                    chart_spec = result.get("chart_spec")
                    if chart_spec and not df.empty:
                        try:
                            chart = alt.Chart(df).mark_bar() if chart_spec.get("mark") == "bar" else alt.Chart(df).mark_line(point=True)

                            x_enc = chart_spec["encoding"]["x"]
                            y_enc = chart_spec["encoding"]["y"]

                            chart = chart.encode(
                                x=alt.X(f"{x_enc['field']}:N" if x_enc.get("type") == "nominal" else f"{x_enc['field']}:O",
                                        sort=None),
                                y=alt.Y(f"{y_enc['field']}:Q"),
                            )

                            if "color" in chart_spec.get("encoding", {}):
                                color_field = chart_spec["encoding"]["color"]["field"]
                                chart = chart.encode(color=f"{color_field}:N")

                            chart = chart.properties(width=700, height=400)
                            st.altair_chart(chart, use_container_width=True)
                        except Exception as e:
                            st.warning(f"Chart rendering error: {e}")

                    # Export button
                    if result.get("export_path"):
                        st.success(f"📁 CSV exported to: {result['export_path']}")
                    elif check_export_allowed(msg_user["role"]):
                        if result.get("results"):
                            csv_data = df.to_csv(index=False)
                            st.download_button(
                                "📥 Export CSV",
                                csv_data,
                                file_name="analytics_export.csv",
                                mime="text/csv",
                                key=f"export_{i}",
                            )
                    else:
                        st.info("Export not available for your role.")

                # Explain button
                if result.get("evidence_pack"):
                    ep = result["evidence_pack"]
                    metric_info = ep.get("metrics", {})
                    with st.expander("💡 Explain this number"):
                        st.markdown(f"**Metric IDs:** {', '.join(metric_info.get('metric_ids', []))}")
                        st.markdown(f"**Metric Versions:** {json.dumps(metric_info.get('metric_versions', {}))}")
                        st.markdown(f"**Data Products:** {', '.join(ep.get('data_products', {}).get('products_used', []))}")
                        st.markdown(f"**SQL Hash:** `{result.get('sql_hash', 'N/A')}`")
                        st.code(result.get("sql", ""), language="sql")

            with tab2:
                # Evidence Pack viewer
                if result.get("evidence_pack"):
                    st.json(result["evidence_pack"])
                    ep_json = json.dumps(result["evidence_pack"], indent=2, default=str)
                    st.download_button(
                        "📥 Download Evidence Pack",
                        ep_json,
                        file_name=f"evidence_pack_{result['evidence_pack'].get('request_id', 'unknown')}.json",
                        mime="application/json",
                        key=f"ep_download_{i}",
                    )

            with tab3:
                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown("**Policy Decision:**")
                    st.json(result.get("policy_decision", {}))
                with col_b:
                    st.markdown("**Quality Status:**")
                    st.json(result.get("quality_info", {}))

                st.markdown(f"**SQL Hash:** `{result.get('sql_hash', '')}`")
                st.code(result.get("sql", ""), language="sql")

        st.markdown("---")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown(
    "**🔒 Security:** All queries are policy-checked before execution. "
    "Evidence packs provide full audit trail."
)
