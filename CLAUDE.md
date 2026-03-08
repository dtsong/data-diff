# data-diff

Cross-database data comparison tool (CLI + Python library). Efficiently diffs rows across 13+ database engines using bisection and checksumming.

## Development Setup

```bash
uv sync                      # install dependencies
uv run pre-commit install    # set up pre-commit hooks
```

## Test Commands

```bash
make test-unit               # unit tests only (no DB required)
make test                    # full suite against PG + MySQL (starts containers)
uv run pytest tests/ -x      # run all tests, stop on first failure
uv run pytest -k <name>      # run a specific test
```

## Linting & Formatting

```bash
uv run ruff check .          # lint
uv run ruff format .         # format
```

Ruff config: `ruff.toml` -- line-length 120, target Python 3.10.

## Code Style

- Format with `ruff format .`; lint with `ruff check .`
- Follow existing patterns in the codebase
- Imports sorted by ruff isort (first-party: `data_diff`)

## Project Structure

- `data_diff/` -- main package
- `data_diff/databases/` -- database driver modules
- `tests/` -- pytest test suite
- `docker-compose.yml` -- local test databases (PG, MySQL, ClickHouse, etc.)

## Contributing

1. Fork and create a feature branch
2. Write tests for changes
3. Ensure `uv run ruff check .` and `uv run pytest` pass
4. Open a PR with a clear description
