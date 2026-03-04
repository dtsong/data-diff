# CODE_MAP.md — data-diff Codebase Navigation

> Quick-reference map for AI agents and new contributors. ~150 lines, dense by design.

## Package Layout

```
data_diff/
├── __init__.py          # Public API: connect_to_table(), diff_tables(), Algorithm
├── __main__.py          # CLI entry point (Click): `data-diff` command
├── version.py           # __version__ = "1.0.0"
├── config.py            # TOML config parsing (--conf flag)
├── errors.py            # Custom exceptions (dbt + core)
├── schema.py            # create_schema() factory, RawColumnInfo, Schema type alias
├── utils.py             # CaseAwareMapping, CaseInsensitiveDict, ArithString, safezip
├── _compat.py           # Compatibility shims (tomllib)
│
├── queries/             # SQL query builder (AST-based)
│   ├── api.py           # User-facing: table(), cte(), join(), leftjoin(), or_()
│   ├── ast_classes.py   # AST nodes: Select, Join, Cte, Column, BinOp, DDL stmts
│   ├── base.py          # SqeletonError, SKIP sentinel, args_as_tuple()
│   └── extras.py        # Checksum, NormalizeAsString (diff-specific query helpers)
│
├── abcs/                # Abstract base classes
│   ├── database_types.py  # DbPath, ColType hierarchy, Collation
│   └── compiler.py        # AbstractCompiler, Compilable protocol
│
├── databases/           # Database drivers (one file per backend)
│   ├── base.py          # Database ABC, BaseDialect, connection pooling
│   ├── _connect.py      # connect(dsn) → Database instance
│   ├── postgresql.py    # PostgreSQL (psycopg2)
│   ├── mysql.py         # MySQL (mysql-connector-python)
│   ├── snowflake.py     # Snowflake (snowflake-connector-python)
│   ├── bigquery.py      # BigQuery (google-cloud-bigquery)
│   ├── redshift.py      # Redshift (extends PostgreSQL)
│   ├── databricks.py    # Databricks (databricks-sql-connector)
│   ├── duckdb.py        # DuckDB (duckdb)
│   ├── clickhouse.py    # ClickHouse (clickhouse-driver)
│   ├── mssql.py         # SQL Server (pyodbc)
│   ├── oracle.py        # Oracle (oracledb)
│   ├── trino.py         # Trino (trino)
│   ├── presto.py        # Presto (presto-python-client)
│   └── vertica.py       # Vertica (vertica-python)
│
├── diff_tables.py       # Algorithm enum, TableDiffer ABC, DiffResultWrapper
├── hashdiff_tables.py   # HashDiffer: cross-DB bisection diff (checksum + download)
├── joindiff_tables.py   # JoinDiffer: same-DB outer-join diff (single query)
├── table_segment.py     # TableSegment: key ranges, split_key_space(), checksums
├── info_tree.py         # InfoTree: hierarchical diff metadata tracking
│
├── thread_utils.py      # PriorityThreadPoolExecutor, ThreadedYielder
├── query_utils.py       # drop_table(), append_to_table() helpers
├── format.py            # Output formatting (JSONL, human-readable)
├── parse_time.py        # Relative time parsing ("5min", "1day")
├── lexicographic_space.py # String key range splitting
│
├── dbt.py               # dbt integration: dbt_diff()
├── dbt_parser.py        # DbtParser: manifest/profile parsing
└── dbt_config_validators.py # dbt config validation
```

## Entry Points

| Entry Point | Location | Description |
|-------------|----------|-------------|
| CLI | `__main__.py:main()` | Click command: `data-diff db1 table1 db2 table2 -k id` |
| Python API | `__init__.py:diff_tables()` | Primary function: takes two `TableSegment`s, returns diff iterator |
| Python API | `__init__.py:connect_to_table()` | Convenience: DSN string → `TableSegment` |
| pyproject.toml | `[project.scripts]` | `data-diff = "data_diff.__main__:main"` |

## Core Data Flow

```
CLI / API call
    │
    ▼
connect(dsn) → Database instance
    │
    ▼
db.query_table_schema() → Schema (column names + types)
    │
    ▼
TableSegment(db, path, key_columns, schema)
    │
    ▼
Algorithm selection (AUTO → JOINDIFF if same-db, else HASHDIFF)
    │
    ├─── HASHDIFF (cross-database) ──────────────────────────┐
    │    1. Checksum full table on both sides                 │
    │    2. If mismatch → bisect key range (factor=32)        │
    │    3. Recurse until segment < threshold (16384 rows)    │
    │    4. Download small segments, compare locally           │
    │    5. diff_sets() → yield ("+", row) / ("-", row)       │
    │                                                         │
    ├─── JOINDIFF (same database) ───────────────────────────┐
    │    1. FULL OUTER JOIN on key columns                    │
    │    2. CASE WHEN to detect exclusive/changed rows        │
    │    3. Optional: materialize results to temp table       │
    │    4. Stream results → yield ("+", row) / ("-", row)    │
    │                                                         │
    ▼
DiffResultWrapper (streaming iterator + stats)
    │
    ▼
Output: human-readable / JSONL / stats summary
```

## Query Builder Architecture

```
api.py (user-facing functions)
    │  table(), cte(), join(), select(), where()
    ▼
ast_classes.py (immutable AST nodes)
    │  Select, Join, Cte, Column, BinOp, Code, ...
    ▼
Database.dialect.compile(compiler, node)
    │  Each driver overrides compilation for its SQL dialect
    ▼
Raw SQL string → db.query(sql)
```

Key pattern: AST nodes are `@attrs.define(frozen=True)` — modifications return new instances via `attrs.evolve()`.

## Test Organization

```
tests/
├── test_query.py          # Query AST construction + CTE schema tests
├── test_sql.py            # SQL generation across dialects
├── test_database.py       # DB integration tests (skip with --ignore)
├── test_diff_tables.py    # Diff framework + threading
├── test_joindiff.py       # JoinDiffer algorithm
├── test_utils.py          # Utility functions (UUID, case-aware dicts)
├── test_thread_utils.py   # PriorityThreadPoolExecutor
├── test_api.py            # Public API surface
├── test_cli.py            # CLI argument parsing
├── test_duckdb.py         # DuckDB-specific
├── test_postgresql.py     # PostgreSQL-specific
├── test_mssql.py          # SQL Server-specific
├── test_parse_time.py     # Time parsing
├── test_datetime_parsing.py # Datetime parsing
├── test_format.py         # Output formatting
├── test_config.py         # TOML config
├── test_dbt*.py           # dbt integration (3 files)
├── test_mesh.py           # Multi-dim segmentation
├── test_main.py           # CLI main function
├── test_database_types.py # Column type system
├── common.py              # Shared fixtures
└── conftest.py            # pytest configuration
```

Run unit tests: `uv run pytest tests/ -x -q --ignore=tests/test_database.py`
Run query tests only: `uv run pytest tests/test_query.py -x -q`

## Key Types

| Type | Location | Purpose |
|------|----------|---------|
| `Schema` | `schema.py` | Type alias for `CaseAwareMapping[str, ColType]` |
| `TableSegment` | `table_segment.py` | Table + key columns + range bounds |
| `DbPath` | `abcs/database_types.py` | `tuple[str, ...]` — schema-qualified table path |
| `Database` | `databases/base.py` | ABC for all database drivers |
| `ExprNode` | `queries/ast_classes.py` | Base class for all query AST nodes |
| `ITable` | `queries/ast_classes.py` | Interface for table-like query nodes |
