.PHONY: demo build-data ingest validate-schema dbt-run dbt-test ge-check promote run-ui test clean start reset

# Default - full demo
demo: build-data ingest promote run-ui

# One-command start (from zero) using Docker Compose
start:
	docker compose up --build

# Reset generated data and artifacts
reset: clean

# Generate synthetic data
build-data:
	SMALL_MODE=1 python scripts/generate_synth_data.py

# Full data build (large dataset)
build-data-full:
	python scripts/generate_synth_data.py

# Ingest data into DuckDB
ingest:
	python scripts/ingest.py

# Validate schemas
validate-schema:
	python scripts/validate_schema.py

# Seed DuckDB
seed:
	python scripts/seed_duckdb.py

# Run dbt models (requires dbt-duckdb)
dbt-run:
	cd dbt && dbt run --profiles-dir .

# Run dbt tests (requires dbt-duckdb)
dbt-test:
	cd dbt && dbt test --profiles-dir .

# Run Great Expectations checks
ge-check:
	python great_expectations/run_checks.py

# Promote data products
promote:
	python scripts/promote.py

# Promote with failures (for testing)
promote-fail-dbt:
	python scripts/promote.py --dbt-failed

promote-fail-ge:
	python scripts/promote.py --ge-failed

# Run Streamlit UI
run-ui:
	streamlit run app/streamlit_app.py --server.port=8501 --server.headless=true

# Run tests
test:
	python -m pytest tests/ -v

# Docker
docker-up:
	docker compose up --build

docker-down:
	docker compose down

# Clean
clean:
	rm -f data/warehouse.duckdb data/warehouse.duckdb.wal
	rm -f data/complaints.csv data/call_reports.csv
	rm -rf artifacts/evidence_packs/*.json
	rm -rf artifacts/exports/*.csv
	rm -rf artifacts/lineage_events/*.json
	rm -rf dbt/target dbt/logs
	rm -rf __pycache__ */__pycache__ */*/__pycache__
