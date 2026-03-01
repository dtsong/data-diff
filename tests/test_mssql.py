from unittest.mock import patch

from data_diff.databases.mssql import MsSQL


def _make_mssql(**extra_kw):
    """Create an MsSQL instance with mocked threading and pyodbc."""
    with patch.object(MsSQL, "__attrs_post_init__", lambda self: None):
        return MsSQL(
            host="localhost",
            port=1433,
            user="sa",
            password="secret",
            database="testdb",
            schema="dbo",
            thread_count=1,
            **extra_kw,
        )


class TestMsSQLConnectionArgs:
    def test_trust_server_certificate_not_set_by_default(self):
        db = _make_mssql()
        assert "TrustServerCertificate" not in db._args

    def test_user_supplied_trust_server_certificate_preserved(self):
        db = _make_mssql(TrustServerCertificate="yes")
        assert db._args["TrustServerCertificate"] == "yes"

    def test_driver_is_set(self):
        db = _make_mssql()
        assert db._args["driver"] == "{ODBC Driver 18 for SQL Server}"

    def test_none_values_filtered_from_args(self):
        db = _make_mssql()
        for v in db._args.values():
            assert v is not None
