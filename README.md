# Verifiable Banking Analytics

A regulated, auditable banking analytics demo. Users ask questions in natural language and receive charts, explanations, and a cryptographically verifiable **Evidence Pack** for compliance.

## What This Is
- Natural-language analytics for banking data
- Policy-first access control (roles, region, purpose)
- Auditable outputs with evidence packs and lineage
- Synthetic data for safe demos and testing

## Architecture

```
NL Query ? Metadata Search ? DSL Plan ? Policy Eval (OPA) ? SQL Compile ? Execute (DuckDB) ? Evidence Pack
```

## Tech Stack
| Component | Technology |
|---|---|
| Analytics Engine | DuckDB |
| Transformations | dbt-core (staging ? gold models) |
| Quality Gates | Great Expectations-style checks |
| Policy Engine | OPA (Rego) with local fallback |
| Agent Orchestration | LangGraph-style state machine |
| SQL Validation | Regex validator + SHA256 hashing |
| Lineage | OpenLineage ? Marquez |
| UI | Streamlit + Altair |
| LLM | Ollama (mock fallback included) |

## Quick Start

### Option 1: Docker (recommended)
```bash
docker compose up --build
```
On first start, the container runs the ingestion pipeline if data products are missing. This keeps quality gates green.

Open `http://localhost:8501` for the UI.

### Option 2: Local development
```bash
pip install -r requirements.txt
make demo
```

Or step-by-step:
```bash
SMALL_MODE=1 python scripts/generate_synth_data.py
python scripts/seed_duckdb.py
python great_expectations/run_checks.py
python scripts/promote.py
streamlit run app/streamlit_app.py
```

## UI Highlights
- Chat-style interface
- Sidebar user profile (role, region, purpose, scenario)
- Query history in sidebar
- Clickable quick queries
- Policy Test mode with a matrix runner

## Data Products
| Data Product | Description | Rows (Small Mode) |
|---|---|---|
| `dp_complaints` | Consumer complaints (CFPB-style) | 50,000 |
| `dp_call_reports` | Quarterly bank financials (FDIC-style) | 720 |

## Roles & Access Control
| Role | LOW | MED | HIGH (Narratives) | Export |
|---|---|---|---|---|
| `branch_manager` | Allow | Deny | Deny | No |
| `risk_officer` | Allow | Allow | Deny | Yes |
| `compliance_officer` | Allow | Allow | Allow w/ constraints | Yes |
| `auditor` | Allow | Allow | Allow w/ constraints | Yes (no HIGH export) |
| `data_analyst` | Allow | Deny | Deny | No |

### High-Sensitivity Constraints
- Must redact/mask narrative text
- Mandatory access logging in evidence pack
- Max rows: 100 (compliance), 50 (auditor)
- Min group size = 10 (k-anonymity)

### Region & Purpose Are Enforced
- **Region** filters data:
  - Complaints have a derived `region`
  - Call reports have `bank_region`
- **Branch manager** must choose a specific region (not `all`)
- **Purpose constraints**:
  - `reporting` ? month aggregation
  - `regulatory` ? quarter aggregation
  - `investigation` ? access logging, row caps, export disabled

### Policy Scenarios (UI)
- Standard (Default)
- Strict K-Anon (min 25)
- Export Locked
- Mask + Redact
- Regional Lockdown
- Max Rows 25

## Metrics Catalog (Expanded)
| Metric | Data Product | Sensitivity |
|---|---|---|
| `complaint_count` | dp_complaints | LOW |
| `timely_response_rate` | dp_complaints | LOW |
| `dispute_rate` | dp_complaints | LOW |
| `narrative_request_count` | dp_complaints | HIGH |
| `net_income_sum` | dp_call_reports | MED |
| `net_income_avg` | dp_call_reports | MED |
| `net_income_margin_avg` | dp_call_reports | MED |
| `deposits_sum` | dp_call_reports | LOW |
| `deposits_avg` | dp_call_reports | LOW |
| `assets_sum` | dp_call_reports | LOW |
| `deposit_to_asset_ratio_avg` | dp_call_reports | LOW |
| `tier1_ratio_avg` | dp_call_reports | LOW |
| `npa_ratio` | dp_call_reports | MED |

## Example Queries
- Show complaint counts by product and state for the last 12 months
- What is the average net income by bank by quarter?
- Timely response rate by region and month
- Deposit to asset ratio by bank region

## Evidence Pack Contents
Each query produces a JSON evidence pack containing:
- `request_id`
- `timestamp`
- `user_attributes` (role, region, purpose)
- `policy_decision` (ALLOW / DENY / ALLOW_WITH_CONSTRAINTS + reason)
- `metrics` (metric IDs + versions)
- `data_products` (products used + versions)
- `data_quality` (freshness + test status)
- `sql` (final + canonical) + SHA256 hash
- `results` (row count, suppression count)
- `lineage` (OpenLineage event ID)
- `export` (artifact path if generated)

## Makefile Targets
```bash
make start            # One command start (Docker)
make demo             # Full local demo
make build-data        # Generate synthetic data (small mode)
make build-data-full   # Generate full 1M row dataset
make ingest            # Run full ingestion pipeline
make validate-schema   # Validate CSV schemas
make seed              # Seed DuckDB
make ge-check          # Run quality checks
make promote           # Promote data products
make run-ui            # Run Streamlit UI
make test              # Run pytest suite
make docker-up         # Docker compose up
make docker-down       # Docker compose down
make clean             # Clean generated files
make reset             # Clean alias
```

## Project Structure
```
+-- docker-compose.yml          # Docker services (app, OPA, Marquez)
+-- Dockerfile                  # App container
+-- Makefile                    # Build targets
+-- requirements.txt            # Python dependencies
+-- data/                       # Synthetic data (CSV) + DuckDB
+-- catalog/                    # Metrics & data product definitions
¦   +-- metrics.yml
¦   +-- data_products.yml
¦   +-- schemas/                # JSON schemas for validation
+-- policies/                   # OPA Rego policies
¦   +-- banking.rego
¦   +-- example_inputs/
+-- dbt/                        # dbt project
¦   +-- models/staging/
¦   +-- models/gold/
¦   +-- tests/
+-- agent/                      # Analytics agent
¦   +-- graph.py
¦   +-- sql_validator.py
¦   +-- policy_client.py
¦   +-- metadata_search.py
¦   +-- evidence.py
¦   +-- quality.py
¦   +-- lineage.py
¦   +-- query_executor.py
+-- app/                        # Streamlit UI
¦   +-- streamlit_app.py
+-- scripts/                    # Utility scripts
¦   +-- generate_synth_data.py
¦   +-- seed_duckdb.py
¦   +-- validate_schema.py
¦   +-- promote.py
¦   +-- ingest.py
¦   +-- run_lineage.py
+-- great_expectations/         # Quality checks
¦   +-- run_checks.py
+-- tests/                      # Pytest test suite
¦   +-- test_banking_analytics.py
+-- artifacts/                  # Generated outputs
    +-- evidence_packs/
    +-- exports/
    +-- lineage_events/
```

## Testing
```bash
python -m pytest tests/ -v
```

## Security Rules
- No PII output
- Narratives are HIGH sensitivity; default deny
- Policy evaluated before SQL generation
- SQL safety: only dp_* tables, no DDL/DML/ATTACH/PRAGMA
- Evidence packs for every response
- Quality gates block querying on failure

## License
Demo/educational only. Not for production use with real financial data.
