# Contributing to data-diff

Contributions are welcome! Whether it is a bug report, feature request, or pull request, we appreciate your help.

## Getting Started

Read through the [README](README.md) and the [documentation](https://data-diff.readthedocs.io/) to understand how data-diff works.

## Reporting Bugs

Please include:

1. The exact command you used (run with `-d` for debug output).
2. The full output (stdout, logs, exceptions).
3. Steps to reproduce the issue, if possible.

Always redact sensitive information like passwords before pasting.

Check [existing issues](https://github.com/datafold/data-diff/issues) before filing a new one.

## Suggesting Enhancements

Open an issue for feature requests, new database support, or documentation improvements. If one already exists, upvote it with a thumbs-up to help prioritize.

## Contributing Code

### Setup

```bash
git clone https://github.com/datafold/data-diff.git
cd data-diff
uv sync                  # install all dependencies
uv run pre-commit install  # set up pre-commit hooks
```

System dependencies (if building native database drivers):

```bash
# macOS
brew install mysql postgresql

# Debian/Ubuntu
apt-get install libpq-dev libmysqlclient-dev
```

### Running Tests

```bash
uv run pytest                    # run all tests
uv run pytest -k <test_name>     # run a specific test
uv run pytest -x                 # stop on first failure
```

### Linting

```bash
uv run ruff check .
```

### Code Style

- Format with `ruff format .`.
- Follow existing patterns in the codebase when in doubt.

### Local Database Setup

Use Docker Compose to spin up test databases:

```bash
docker-compose up -d mysql postgres
```

Update `TEST_*_CONN_STRING` values in `tests/local_settings.py` (git-ignored) if your setup differs from the defaults.

### Implementing a New Database

Add a new module in `data_diff/databases/`. If possible, include the database in `docker-compose.yml` and update CI configuration.

Guide: https://data-diff.readthedocs.io/en/latest/new-database-driver-guide.html

## Pull Request Process

1. Fork and create a feature branch.
2. Write tests for your changes.
3. Ensure all tests pass and linting is clean.
4. Open a PR with a clear description of the change.

Maintainers will review and provide feedback. See [GOVERNANCE.md](GOVERNANCE.md) for how decisions are made.
