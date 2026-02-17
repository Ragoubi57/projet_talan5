# 🏦 Verifiable Banking Analytics

A regulated, auditable banking analytics demo system. Users ask analytics questions in natural language and receive charts, explanations, and cryptographically verifiable **Evidence Packs** for audit compliance.

## Architecture

```
NL Query → Metadata Search → DSL Plan → Policy Eval (OPA) → SQL Compile → Execute (DuckDB) → Evidence Pack
```

| Component | Technology |
|-----------|-----------|
| Analytics Engine | DuckDB |
| Transformations | dbt-core (staging → gold models) |
| Quality Gates | Great Expectations-style checks |
| Policy Engine | OPA (Rego) with local fallback |
| Agent Orchestration | LangGraph-style state machine |
| SQL Validation | Regex-based validator + SHA256 hashing |
| Lineage | OpenLineage → Marquez |
| UI | Streamlit + Altair charts |
| LLM | Ollama (mock LLM fallback included) |

## Quick Start

### Option 1: Docker Compose (recommended)

```bash
docker compose up --build
```

Open http://localhost:8501 for the Streamlit UI.

### Option 2: Local Development

```bash
pip install -r requirements.txt
make demo
```

Or step by step:

```bash
SMALL_MODE=1 python scripts/generate_synth_data.py
python scripts/seed_duckdb.py
python great_expectations/run_checks.py
python scripts/promote.py
streamlit run app/streamlit_app.py
```

## Data Products

| Data Product | Description | Rows (Small Mode) |
|-------------|-------------|-------------------|
| `dp_complaints` | Consumer complaints (CFPB-style) | 50,000 |
| `dp_call_reports` | Quarterly bank financials (FDIC-style) | 720 |

### Synthetic Data Generation

```bash
SMALL_MODE=1 python scripts/generate_synth_data.py   # 50K complaints
python scripts/generate_synth_data.py                  # 1M complaints (default)
```

## Roles & Access Control

| Role | LOW Data | MED Data | HIGH Data (Narratives) | Export |
|------|----------|----------|----------------------|--------|
| `branch_manager` | ✅ Allow | ❌ Deny | ❌ Deny | ❌ |
| `risk_officer` | ✅ Allow | ✅ Allow | ❌ Deny | ✅ |
| `compliance_officer` | ✅ Allow | ✅ Allow | ⚠️ Allow w/ Constraints | ✅ |
| `auditor` | ✅ Allow | ✅ Allow | ⚠️ Allow w/ Constraints | ✅ (no HIGH) |
| `data_analyst` | ✅ Allow | ❌ Deny | ❌ Deny | ❌ |

**Constraints for HIGH sensitivity access:**
- Must redact/mask narrative text
- Mandatory access logging in evidence pack
- Max rows limited (100 for compliance, 50 for auditor)
- Min group size ≥ 10 (k-anonymity)

## Metrics Catalog

| Metric | Data Product | Sensitivity |
|--------|-------------|-------------|
| `complaint_count` | dp_complaints | LOW |
| `net_income_sum` | dp_call_reports | MED |
| `net_income_avg` | dp_call_reports | MED |
| `deposits_sum` | dp_call_reports | LOW |
| `tier1_ratio_avg` | dp_call_reports | LOW |
| `npa_ratio` | dp_call_reports | MED |

## Example Queries

### Branch Manager
```
Show complaint counts by product and state for the last 12 months
→ ✅ Returns aggregated data with min_group_size=10 enforced
```

### Risk Officer
```
What is the average net income by bank by quarter?
→ ✅ Returns quarterly financial data
```

### Compliance Officer
```
Show me complaint narratives for investigations
→ ⚠️ ALLOW_WITH_CONSTRAINTS: narratives are redacted, access logged
```

### Branch Manager (denied)
```
Show me complaint narratives
→ ❌ DENIED: High sensitivity data not available for this role
```

## Evidence Pack

Every query produces a JSON evidence pack containing:

- `request_id` - Unique identifier
- `timestamp` - ISO 8601 timestamp
- `user_attributes` - Role, region, purpose
- `policy_decision` - ALLOW/DENY/ALLOW_WITH_CONSTRAINTS + reason
- `metrics` - Metric IDs and versions used
- `data_products` - Products used with versions
- `data_quality` - Freshness and test status
- `sql` - Final SQL, canonical SQL, SHA256 hash
- `results` - Row count, suppression count
- `lineage` - OpenLineage event ID
- `export` - Export artifact path (if generated)

## Makefile Targets

```bash
make demo              # Full demo: generate data, ingest, promote, run UI
make build-data        # Generate synthetic data (small mode)
make build-data-full   # Generate full 1M row dataset
make ingest            # Run full ingestion pipeline
make validate-schema   # Validate CSV schemas
make seed              # Seed DuckDB
make ge-check          # Run quality checks
make promote           # Promote data products
make promote-fail-dbt  # Test: promote with dbt failure
make promote-fail-ge   # Test: promote with GE failure
make run-ui            # Run Streamlit UI
make test              # Run pytest suite
make docker-up         # Docker compose up
make docker-down       # Docker compose down
make clean             # Clean generated files
```

## Project Structure

```
├── docker-compose.yml          # Docker services (app, OPA, Marquez)
├── Dockerfile                  # App container
├── Makefile                    # Build targets
├── requirements.txt            # Python dependencies
├── data/                       # Synthetic data (CSV) + DuckDB
├── catalog/                    # Metrics & data product definitions
│   ├── metrics.yml
│   ├── data_products.yml
│   ├── schemas/                # JSON schemas for validation
│   └── loader.py               # Python catalog loader
├── policies/                   # OPA Rego policies
│   ├── banking.rego
│   └── example_inputs/         # Example policy inputs
├── dbt/                        # dbt project
│   ├── models/staging/         # stg_complaints, stg_call_reports
│   ├── models/gold/            # dp_complaints, dp_call_reports
│   └── tests/
├── agent/                      # Analytics agent
│   ├── graph.py                # LangGraph-style orchestration
│   ├── sql_validator.py        # SQL validation + hashing
│   ├── policy_client.py        # OPA client + local fallback
│   ├── metadata_search.py      # Metric/data product search
│   ├── evidence.py             # Evidence pack creation
│   ├── quality.py              # Quality status checks
│   ├── lineage.py              # OpenLineage events
│   └── query_executor.py       # DuckDB execution
├── app/                        # Streamlit UI
│   └── streamlit_app.py
├── scripts/                    # Utility scripts
│   ├── generate_synth_data.py  # Synthetic data generator
│   ├── seed_duckdb.py          # Load CSV → DuckDB
│   ├── validate_schema.py      # Schema validation
│   ├── promote.py              # Data product promotion
│   ├── ingest.py               # Full ingestion pipeline
│   └── run_lineage.py          # View lineage events
├── great_expectations/         # Quality checks
│   └── run_checks.py
├── tests/                      # Pytest test suite
│   └── test_banking_analytics.py
└── artifacts/                  # Generated outputs
    ├── evidence_packs/
    ├── exports/
    └── lineage_events/
```

## Testing

```bash
python -m pytest tests/ -v
```

42 tests covering:
- Policy deny narratives for unauthorized roles
- SQL validator blocks raw/staging tables
- Evidence pack required fields and SQL hash stability
- Promote blocking on failed quality checks
- Catalog loading and metadata search
- Export permission checks

## Security Rules

- **No PII output** - No raw IDs beyond synthetic complaint_id/bank_id
- **Narratives are HIGH sensitivity** - Default deny
- **Policy-first** - POLICY_EVAL runs before SQL generation
- **Safe defaults** - Ambiguous requests use most aggregated interpretation
- **Min group size** - Global k-anonymity enforcement (default: 10)
- **SQL safety** - Only dp_* tables allowed; no PRAGMA, ATTACH, file reads, DDL/DML
- **Audit trail** - Every response includes evidence pack
- **Quality gating** - Failed quality checks prevent querying

## License

This is a demo/educational system. Not for production use with real financial data.