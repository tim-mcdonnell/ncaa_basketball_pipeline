.PHONY: clean validate duckdb espn prep dev help

# To persist data during clean, run: make clean PERSIST_DATA=true
# PERSIST_DATA is undefined by default, leading to data deletion.
PERSIST_DATA ?=

clean:
	@echo "Cleaning cache directories..."
	# Exclude venv directory from find operations
	find . -path ./venv -prune -o -name "__pycache__" -type d -exec rm -rf {} +
	find . -path ./venv -prune -o -name ".pytest_cache" -type d -exec rm -rf {} +
	find . -path ./venv -prune -o -name ".ruff_cache" -type d -exec rm -rf {} +
	find . -path ./venv -prune -o -name ".mypy_cache" -type d -exec rm -rf {} +
	@echo "Cache directories cleaned."
ifeq ($(PERSIST_DATA),)
	@echo "Deleting contents of data/ folder..."
	@if [ -d "data" ]; then \
		find data/ -mindepth 1 -delete; \
		echo "Contents of data/ folder deleted."; \
	else \
		echo "data/ directory not found, skipping deletion of its contents."; \
	fi
else
	@echo "PERSIST_DATA is set. Skipping deletion of data/ contents."
endif
	@echo "Clean operation finished."

validate:
	@echo "Validating Dagster definitions..."
	dagster definitions validate
	@echo "Validation complete."

show:
	@echo "Fetching all tables from the database..."
	@DB_PATH="data/ncaa_basketball.duckdb"; \
	duckdb "$$DB_PATH" "COPY (SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema != 'information_schema' ORDER BY table_schema, table_name) TO '/dev/stdout' WITH (FORMAT csv, HEADER);" | tail -n +2 | while IFS=',' read -r SCHEMA TABLE; do \
		if [ -z "$$SCHEMA" ] || [ -z "$$TABLE" ]; then \
			continue; \
		fi; \
		SCHEMA=$$(echo "$$SCHEMA" | xargs); \
		TABLE=$$(echo "$$TABLE" | xargs); \
		echo "=========================================================="; \
		echo "Table: $$SCHEMA.$$TABLE"; \
		echo "=========================================================="; \
		duckdb "$$DB_PATH" "SELECT * FROM \\"$$SCHEMA\\".\\"$$TABLE\\" LIMIT 5;"; \
		echo ""; \
	done
	@echo "Finished processing all tables."

prep:
	@echo "Staging all changes and running pre-commit hooks..."
	git add .
	pre-commit run -a
	@echo "Pre-commit checks finished."

dev:
	@echo "Starting Dagster dev environment..."
	dagster dev

help:
	@echo "Available targets:"
	@echo "  make clean                Clean cache directories and (by default) delete 'data/' contents."
	@echo "                            To preserve 'data/' contents, run: make clean PERSIST_DATA=true"
	@echo "  make validate             Validate Dagster definitions (runs 'dagster definitions validate')."
	@echo "  make show                 Show tables and sample data from the DuckDB database."
	@echo "  make prep                 Stage all changes and run all pre-commit hooks."
	@echo "  make dev                  Start the Dagster development environment."
	@echo "  make help                 Show this help message."

# Set help as the default goal if no target is specified
.DEFAULT_GOAL := help 