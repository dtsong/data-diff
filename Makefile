.PHONY: up up-full down test-unit test demo

## Start PostgreSQL + MySQL (lightweight, fast startup)
up:
	docker compose up -d --wait postgres mysql

## Start all services including ClickHouse, Presto, Trino, Vertica
up-full:
	docker compose --profile full up -d --wait

## Stop all services and remove volumes
down:
	docker compose --profile full down -v

## Run unit tests (no database required)
test-unit:
	uv run pytest tests/test_query.py tests/test_utils.py -x

## Run full test suite against PG + MySQL (starts containers if needed)
test: up
	uv run pytest tests/ \
		-o addopts="--timeout=300 --tb=short" \
		--ignore=tests/test_database_types.py \
		--ignore=tests/test_dbt_config_validators.py \
		--ignore=tests/test_main.py

## Run data-diff against seed data to showcase diffing
demo: up
	@echo "=== PostgreSQL: ratings_source vs ratings_target ==="
	uv run python -m data_diff \
		postgresql://postgres:Password1@localhost/postgres \
		ratings_source ratings_target \
		--key-columns id \
		--columns rating
	@echo ""
	@echo "=== MySQL: ratings_source vs ratings_target ==="
	uv run python -m data_diff \
		mysql://mysql:Password1@localhost/mysql \
		ratings_source ratings_target \
		--key-columns id \
		--columns rating
