"""Shared pytest fixtures for data-diff tests."""

import uuid

import pytest


@pytest.fixture
def duckdb_connection():
    """Provide a DuckDB connection for tests that need a local database.

    Uses an in-memory DuckDB instance via the standard connection string.
    """
    from data_diff import databases as db
    from tests.common import get_conn

    conn = get_conn(db.DuckDB)
    yield conn


@pytest.fixture
def duckdb_file_connection(tmp_path):
    """Provide a file-backed DuckDB connection that is cleaned up after the test."""
    from data_diff.databases import duckdb as duckdb_mod

    filepath = str(tmp_path / f"{uuid.uuid4()}.duckdb")
    conn = duckdb_mod.DuckDB(filepath=filepath)
    yield conn
    # Cleanup handled by tmp_path
