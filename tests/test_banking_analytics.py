"""Test suite for Verifiable Banking Analytics.

Tests:
- Policy deny narratives for unauthorized roles
- SQL validator blocks raw tables
- Evidence pack contains required fields and SQL hash stability
- Promote step blocks dp_* on failed quality checks
"""
import os
import sys
import json
import pytest

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from agent.sql_validator import validate_sql, hash_sql, normalize_sql, apply_min_group_size
from agent.policy_client import policy_eval, _local_policy_eval, check_export_allowed
from agent.evidence import make_evidence_pack
from agent.metadata_search import metadata_search, get_metric_details
from catalog.loader import load_metrics, load_data_products, get_sensitive_columns


# ============================================================
# Test: Policy deny narratives for unauthorized roles
# ============================================================

class TestPolicyDenyNarratives:
    """Test that policy denies narrative access for unauthorized roles."""

    def test_branch_manager_denied_narratives(self):
        """Branch manager should be denied access to HIGH sensitivity narratives."""
        request = {
            "user": {"role": "branch_manager", "region": "northeast", "purpose": "reporting"},
            "data_product": "dp_complaints",
            "columns": [
                {"name": "consumer_narrative", "sensitivity": "HIGH"},
                {"name": "complaint_id", "sensitivity": "LOW"},
            ],
            "action": "query",
        }
        decision = _local_policy_eval(request)
        assert decision["result"] == "DENY"
        assert "high sensitivity" in decision["reason"].lower() or "denied" in decision["reason"].lower()

    def test_risk_officer_denied_narratives(self):
        """Risk officer should be denied access to HIGH sensitivity narratives."""
        request = {
            "user": {"role": "risk_officer", "region": "all", "purpose": "analysis"},
            "data_product": "dp_complaints",
            "columns": [
                {"name": "consumer_narrative", "sensitivity": "HIGH"},
            ],
            "action": "query",
        }
        decision = _local_policy_eval(request)
        assert decision["result"] == "DENY"

    def test_data_analyst_denied_narratives(self):
        """Data analyst should be denied access to HIGH sensitivity narratives."""
        request = {
            "user": {"role": "data_analyst", "region": "all", "purpose": "analysis"},
            "data_product": "dp_complaints",
            "columns": [
                {"name": "consumer_narrative", "sensitivity": "HIGH"},
            ],
            "action": "query",
        }
        decision = _local_policy_eval(request)
        assert decision["result"] == "DENY"

    def test_compliance_officer_allowed_narratives_with_constraints(self):
        """Compliance officer should be allowed narratives WITH constraints."""
        request = {
            "user": {"role": "compliance_officer", "region": "all", "purpose": "investigation"},
            "data_product": "dp_complaints",
            "columns": [
                {"name": "consumer_narrative", "sensitivity": "HIGH"},
                {"name": "complaint_id", "sensitivity": "LOW"},
            ],
            "action": "query",
        }
        decision = _local_policy_eval(request)
        assert decision["result"] == "ALLOW_WITH_CONSTRAINTS"
        assert decision["constraints"].get("must_redact_narratives") is True
        assert decision["constraints"].get("must_log_access") is True
        assert decision["constraints"].get("must_mask") is True

    def test_auditor_allowed_narratives_with_constraints(self):
        """Auditor should get ALLOW_WITH_CONSTRAINTS for narratives."""
        request = {
            "user": {"role": "auditor", "region": "all", "purpose": "audit"},
            "data_product": "dp_complaints",
            "columns": [
                {"name": "consumer_narrative", "sensitivity": "HIGH"},
            ],
            "action": "query",
        }
        decision = _local_policy_eval(request)
        assert decision["result"] == "ALLOW_WITH_CONSTRAINTS"
        assert decision["constraints"].get("forbid_export") is True

    def test_branch_manager_allowed_low_sensitivity(self):
        """Branch manager should be allowed LOW sensitivity data."""
        request = {
            "user": {"role": "branch_manager", "region": "northeast", "purpose": "reporting"},
            "data_product": "dp_complaints",
            "columns": [
                {"name": "product", "sensitivity": "LOW"},
                {"name": "state", "sensitivity": "LOW"},
            ],
            "action": "query",
        }
        decision = _local_policy_eval(request)
        assert decision["result"] == "ALLOW"

    def test_risk_officer_allowed_med_sensitivity(self):
        """Risk officer should be allowed MED sensitivity data."""
        request = {
            "user": {"role": "risk_officer", "region": "all", "purpose": "analysis"},
            "data_product": "dp_call_reports",
            "columns": [
                {"name": "net_income", "sensitivity": "MED"},
                {"name": "quarter", "sensitivity": "LOW"},
            ],
            "action": "query",
        }
        decision = _local_policy_eval(request)
        assert decision["result"] == "ALLOW"

    def test_branch_manager_denied_med_sensitivity(self):
        """Branch manager should be denied MED sensitivity data."""
        request = {
            "user": {"role": "branch_manager", "region": "northeast", "purpose": "reporting"},
            "data_product": "dp_call_reports",
            "columns": [
                {"name": "net_income", "sensitivity": "MED"},
            ],
            "action": "query",
        }
        decision = _local_policy_eval(request)
        assert decision["result"] == "DENY"

    def test_unknown_role_denied(self):
        """Unknown role should be denied."""
        request = {
            "user": {"role": "unknown_role", "region": "all", "purpose": "analysis"},
            "data_product": "dp_complaints",
            "columns": [{"name": "product", "sensitivity": "LOW"}],
            "action": "query",
        }
        decision = _local_policy_eval(request)
        assert decision["result"] == "DENY"


# ============================================================
# Test: SQL validator blocks raw tables
# ============================================================

class TestSQLValidator:
    """Test that SQL validator blocks access to raw tables and dangerous SQL."""

    def test_allows_dp_tables(self):
        """SQL referencing dp_* tables should be allowed."""
        sql = "SELECT product, COUNT(*) FROM dp_complaints GROUP BY product"
        is_valid, error = validate_sql(sql)
        assert is_valid is True
        assert error == ""

    def test_blocks_raw_tables(self):
        """SQL referencing raw tables should be blocked."""
        sql = "SELECT * FROM raw_complaints"
        is_valid, error = validate_sql(sql)
        assert is_valid is False
        assert "raw_complaints" in error

    def test_blocks_stg_tables(self):
        """SQL referencing staging tables should be blocked."""
        sql = "SELECT * FROM stg_complaints"
        is_valid, error = validate_sql(sql)
        assert is_valid is False
        assert "stg_complaints" in error

    def test_blocks_pragma(self):
        """PRAGMA statements should be blocked."""
        sql = "PRAGMA database_list"
        is_valid, error = validate_sql(sql)
        assert is_valid is False
        assert "PRAGMA" in error

    def test_blocks_attach(self):
        """ATTACH statements should be blocked."""
        sql = "ATTACH DATABASE 'other.db' AS other"
        is_valid, error = validate_sql(sql)
        assert is_valid is False

    def test_blocks_create(self):
        """CREATE statements should be blocked."""
        sql = "CREATE TABLE test (id INT)"
        is_valid, error = validate_sql(sql)
        assert is_valid is False

    def test_blocks_drop(self):
        """DROP statements should be blocked."""
        sql = "DROP TABLE dp_complaints"
        is_valid, error = validate_sql(sql)
        assert is_valid is False

    def test_blocks_insert(self):
        """INSERT statements should be blocked."""
        sql = "INSERT INTO dp_complaints VALUES (1, '2024-01-01')"
        is_valid, error = validate_sql(sql)
        assert is_valid is False

    def test_blocks_delete(self):
        """DELETE statements should be blocked."""
        sql = "DELETE FROM dp_complaints"
        is_valid, error = validate_sql(sql)
        assert is_valid is False

    def test_blocks_file_reads(self):
        """Direct file reads should be blocked."""
        sql = "SELECT * FROM read_csv_auto('/etc/passwd')"
        is_valid, error = validate_sql(sql)
        assert is_valid is False

    def test_sql_hash_stability(self):
        """SQL hash should be stable for equivalent queries."""
        sql1 = "SELECT product, COUNT(*) FROM dp_complaints GROUP BY product"
        sql2 = "SELECT  product,  COUNT(*)  FROM  dp_complaints  GROUP  BY  product"
        assert hash_sql(sql1) == hash_sql(sql2)

    def test_sql_hash_different_for_different_queries(self):
        """Different queries should have different hashes."""
        sql1 = "SELECT product FROM dp_complaints"
        sql2 = "SELECT state FROM dp_complaints"
        assert hash_sql(sql1) != hash_sql(sql2)

    def test_min_group_size_applied(self):
        """Min group size HAVING clause should be applied to GROUP BY queries."""
        sql = "SELECT product, COUNT(*) FROM dp_complaints GROUP BY product"
        result = apply_min_group_size(sql, 10)
        assert "HAVING COUNT(*) >= 10" in result

    def test_min_group_size_not_applied_without_group_by(self):
        """Min group size should not be applied to queries without GROUP BY."""
        sql = "SELECT COUNT(*) FROM dp_complaints"
        result = apply_min_group_size(sql, 10)
        assert "HAVING" not in result

    def test_allows_dp_call_reports(self):
        """dp_call_reports should be allowed."""
        sql = "SELECT quarter, AVG(net_income) FROM dp_call_reports GROUP BY quarter"
        is_valid, error = validate_sql(sql)
        assert is_valid is True


# ============================================================
# Test: Evidence pack contains required fields and SQL hash stable
# ============================================================

class TestEvidencePack:
    """Test evidence pack creation and contents."""

    def test_evidence_pack_required_fields(self):
        """Evidence pack must contain all required fields."""
        ep = make_evidence_pack(
            request_text="Show complaint counts",
            user_attrs={"role": "branch_manager", "region": "northeast", "purpose": "reporting"},
            policy_decision={"result": "ALLOW", "reason": "Allowed", "constraints": {"min_group_size": 10}},
            metric_ids=["complaint_count"],
            metric_versions={"complaint_count": "1.2.0"},
            data_products_used=["dp_complaints"],
            data_product_versions={"dp_complaints": "2.0.0"},
            freshness_status={"dp_complaints": "2024-01-01T00:00:00Z"},
            quality_status={"dp_complaints": True},
            sql_text="SELECT product, COUNT(*) FROM dp_complaints GROUP BY product",
            canonical_sql="SELECT PRODUCT, COUNT(*) FROM DP_COMPLAINTS GROUP BY PRODUCT",
            sql_hash="abc123",
            row_count=10,
            suppression_count=2,
            lineage_event_id="lineage-123",
        )

        # Check required fields
        assert "request_id" in ep
        assert "timestamp" in ep
        assert "request_text" in ep
        assert "user_attributes" in ep
        assert "policy_decision" in ep
        assert "metrics" in ep
        assert "data_products" in ep
        assert "data_quality" in ep
        assert "sql" in ep
        assert "results" in ep
        assert "lineage" in ep
        assert "export" in ep

        # Check nested fields
        assert ep["user_attributes"]["role"] == "branch_manager"
        assert ep["policy_decision"]["result"] == "ALLOW"
        assert "complaint_count" in ep["metrics"]["metric_ids"]
        assert ep["metrics"]["metric_versions"]["complaint_count"] == "1.2.0"
        assert "dp_complaints" in ep["data_products"]["products_used"]
        assert ep["sql"]["sql_hash"] == "abc123"
        assert ep["results"]["row_count"] == 10
        assert ep["results"]["suppression_count"] == 2
        assert ep["lineage"]["event_id"] == "lineage-123"

    def test_evidence_pack_sql_hash_stable(self):
        """SQL hash in evidence pack should be stable for same query."""
        sql = "SELECT product, COUNT(*) FROM dp_complaints GROUP BY product"
        hash1 = hash_sql(sql)
        hash2 = hash_sql(sql)
        assert hash1 == hash2

        # Create two evidence packs with same SQL
        ep1 = make_evidence_pack(
            request_text="test", user_attrs={"role": "test"},
            policy_decision={"result": "ALLOW", "reason": "test", "constraints": {}},
            metric_ids=["test"], metric_versions={},
            data_products_used=["dp_complaints"], data_product_versions={},
            freshness_status={}, quality_status={},
            sql_text=sql, canonical_sql=normalize_sql(sql),
            sql_hash=hash1, row_count=0,
        )
        ep2 = make_evidence_pack(
            request_text="test", user_attrs={"role": "test"},
            policy_decision={"result": "ALLOW", "reason": "test", "constraints": {}},
            metric_ids=["test"], metric_versions={},
            data_products_used=["dp_complaints"], data_product_versions={},
            freshness_status={}, quality_status={},
            sql_text=sql, canonical_sql=normalize_sql(sql),
            sql_hash=hash2, row_count=0,
        )
        assert ep1["sql"]["sql_hash"] == ep2["sql"]["sql_hash"]

    def test_evidence_pack_written_to_disk(self):
        """Evidence pack should be written to artifacts directory."""
        ep = make_evidence_pack(
            request_text="test disk write", user_attrs={"role": "test"},
            policy_decision={"result": "ALLOW", "reason": "test", "constraints": {}},
            metric_ids=["test"], metric_versions={},
            data_products_used=[], data_product_versions={},
            freshness_status={}, quality_status={},
            sql_text="SELECT 1", canonical_sql="SELECT 1",
            sql_hash="test", row_count=0,
        )
        filepath = os.path.join(
            PROJECT_ROOT, "artifacts", "evidence_packs", f"{ep['request_id']}.json"
        )
        assert os.path.exists(filepath)
        with open(filepath) as f:
            loaded = json.load(f)
        assert loaded["request_id"] == ep["request_id"]
        # Clean up
        os.remove(filepath)


# ============================================================
# Test: Promote step blocks dp_* on failed GE/dbt tests
# ============================================================

class TestPromoteBlocking:
    """Test that promote step blocks data products on failed quality checks."""

    def test_promote_blocks_on_dbt_failure(self):
        """Promote should not promote when dbt tests fail."""
        import duckdb
        db_path = os.path.join(PROJECT_ROOT, "data", "test_promote.duckdb")
        conn = duckdb.connect(db_path)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS promote_status (
                data_product VARCHAR PRIMARY KEY,
                promoted BOOLEAN DEFAULT FALSE,
                last_promoted VARCHAR,
                dbt_passed BOOLEAN DEFAULT FALSE,
                ge_passed BOOLEAN DEFAULT FALSE
            )
        """)
        conn.execute("""
            INSERT OR REPLACE INTO promote_status VALUES
                ('dp_complaints', FALSE, NULL, FALSE, TRUE)
        """)

        result = conn.execute(
            "SELECT promoted FROM promote_status WHERE data_product = 'dp_complaints'"
        ).fetchone()
        assert result[0] is False

        conn.close()
        os.remove(db_path)

    def test_promote_blocks_on_ge_failure(self):
        """Promote should not promote when GE checks fail."""
        import duckdb
        db_path = os.path.join(PROJECT_ROOT, "data", "test_promote_ge.duckdb")
        conn = duckdb.connect(db_path)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS promote_status (
                data_product VARCHAR PRIMARY KEY,
                promoted BOOLEAN DEFAULT FALSE,
                last_promoted VARCHAR,
                dbt_passed BOOLEAN DEFAULT FALSE,
                ge_passed BOOLEAN DEFAULT FALSE
            )
        """)
        conn.execute("""
            INSERT OR REPLACE INTO promote_status VALUES
                ('dp_call_reports', FALSE, NULL, TRUE, FALSE)
        """)

        result = conn.execute(
            "SELECT promoted FROM promote_status WHERE data_product = 'dp_call_reports'"
        ).fetchone()
        assert result[0] is False

        conn.close()
        os.remove(db_path)

    def test_promote_allows_when_all_pass(self):
        """Promote should promote when both dbt and GE pass."""
        import duckdb
        db_path = os.path.join(PROJECT_ROOT, "data", "test_promote_pass.duckdb")
        conn = duckdb.connect(db_path)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS promote_status (
                data_product VARCHAR PRIMARY KEY,
                promoted BOOLEAN DEFAULT FALSE,
                last_promoted VARCHAR,
                dbt_passed BOOLEAN DEFAULT FALSE,
                ge_passed BOOLEAN DEFAULT FALSE
            )
        """)
        conn.execute("""
            INSERT OR REPLACE INTO promote_status VALUES
                ('dp_complaints', TRUE, '2024-01-01', TRUE, TRUE)
        """)

        result = conn.execute(
            "SELECT promoted, dbt_passed, ge_passed FROM promote_status WHERE data_product = 'dp_complaints'"
        ).fetchone()
        assert result[0] is True
        assert result[1] is True
        assert result[2] is True

        conn.close()
        os.remove(db_path)


# ============================================================
# Test: Catalog and metadata
# ============================================================

class TestCatalog:
    """Test catalog loading and metadata search."""

    def test_load_metrics(self):
        """Should load all defined metrics."""
        metrics = load_metrics()
        assert len(metrics) >= 5
        metric_ids = [m["metric_id"] for m in metrics]
        assert "complaint_count" in metric_ids
        assert "net_income_sum" in metric_ids
        assert "deposits_sum" in metric_ids
        assert "tier1_ratio_avg" in metric_ids

    def test_load_data_products(self):
        """Should load all defined data products."""
        dps = load_data_products()
        assert len(dps) >= 2
        dp_ids = [dp["id"] for dp in dps]
        assert "dp_complaints" in dp_ids
        assert "dp_call_reports" in dp_ids

    def test_sensitive_columns(self):
        """Should identify HIGH sensitivity columns."""
        sensitive = get_sensitive_columns("dp_complaints")
        assert "consumer_narrative" in sensitive

    def test_metadata_search_complaints(self):
        """Should find complaint-related metrics."""
        results = metadata_search("complaint counts by product")
        assert len(results["metrics"]) > 0
        assert results["suggested_metric"] == "complaint_count"

    def test_metadata_search_income(self):
        """Should find income-related metrics."""
        results = metadata_search("net income by bank")
        assert len(results["metrics"]) > 0
        assert "net_income" in results["suggested_metric"]

    def test_metadata_search_deposits(self):
        """Should find deposit-related metrics."""
        results = metadata_search("total deposits")
        assert len(results["metrics"]) > 0

    def test_metric_details(self):
        """Should return full metric details."""
        metric = get_metric_details("complaint_count")
        assert metric is not None
        assert metric["metric_id"] == "complaint_count"
        assert "version" in metric
        assert "sql_template" in metric


# ============================================================
# Test: Export permissions
# ============================================================

class TestExportPermissions:
    """Test export permission checks."""

    def test_branch_manager_cannot_export(self):
        assert check_export_allowed("branch_manager") is False

    def test_risk_officer_can_export(self):
        assert check_export_allowed("risk_officer") is True

    def test_compliance_officer_can_export(self):
        assert check_export_allowed("compliance_officer") is True

    def test_data_analyst_cannot_export(self):
        assert check_export_allowed("data_analyst") is False

    def test_auditor_can_export(self):
        assert check_export_allowed("auditor") is True
