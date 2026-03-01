# data-diff -- Efficiently diff rows across databases

[![Community Maintained](https://img.shields.io/badge/maintained-community-blue)](https://github.com/dtsong/data-diff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/data-diff)](https://pypi.org/project/data-diff/)

> **Note:** This project is maintained by the community after Datafold sunset the project in May 2024.

**data-diff** is an open-source CLI and Python library for efficiently comparing data across 13+ database engines. It uses bisection and checksumming to find differing rows without transferring entire tables, making it fast even on tables with millions of rows.

## Installation

```bash
pip install data-diff
```

Install with database-specific extras:

```bash
pip install 'data-diff[postgresql,mysql]'
```

## Quick Start

### CLI

```bash
data-diff \
  postgresql://user:password@localhost/db1 table1 \
  postgresql://user:password@localhost/db2 table2 \
  --key-columns id \
  --columns name,email,updated_at
```

### Python API

```python
import data_diff

diff = data_diff.diff_tables(
    table1=data_diff.connect_to_table("postgresql://localhost/db1", "table1", "id"),
    table2=data_diff.connect_to_table("postgresql://localhost/db2", "table2", "id"),
)

for sign, row in diff:
    print(sign, row)  # '+' for added, '-' for removed
```

## Supported Databases

| Database     | Status |
|-------------|--------|
| PostgreSQL  | Supported |
| MySQL       | Supported |
| Snowflake   | Supported |
| BigQuery    | Supported |
| Databricks  | Supported |
| Redshift    | Supported |
| DuckDB      | Supported |
| Presto      | Supported |
| Trino       | Supported |
| Oracle      | Supported |
| MS SQL      | Supported |
| ClickHouse  | Supported |
| Vertica     | Supported |

## dbt Integration

data-diff integrates with [dbt](https://www.getdbt.com/) to compare tables between development and production environments:

```bash
data-diff --dbt
```

Install with dbt support:

```bash
pip install 'data-diff[dbt]'
```

See the [full documentation](https://data-diff.readthedocs.io/) for configuration details.

## Documentation

- [Full Documentation](https://data-diff.readthedocs.io/)
- [Contributing Guide](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)
- [Governance](GOVERNANCE.md)

## Contributors

<a href="https://github.com/dtsong/data-diff/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=dtsong/data-diff" />
</a>

## License

This project is licensed under the terms of the [MIT License](LICENSE).
