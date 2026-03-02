from unittest.mock import MagicMock, patch

import pytest

from data_diff.databases.base import ConnectError
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


class TestMsSQLConnectionErrors:
    def test_ssl_error_provides_actionable_message(self):
        db = _make_mssql()
        mock_mssql = MagicMock()
        mock_mssql.Error = type("Error", (Exception,), {})
        mock_mssql.connect.side_effect = mock_mssql.Error(
            "[SSL Provider] The certificate chain was issued by an untrusted authority"
        )

        with patch("data_diff.databases.mssql.import_mssql", return_value=mock_mssql):
            with pytest.raises(ConnectError, match="TrustServerCertificate"):
                db.create_connection()

    def test_certificate_only_keyword_triggers_actionable_message(self):
        db = _make_mssql()
        mock_mssql = MagicMock()
        mock_mssql.Error = type("Error", (Exception,), {})
        mock_mssql.connect.side_effect = mock_mssql.Error(
            "The remote certificate was rejected by the verification procedure"
        )

        with patch("data_diff.databases.mssql.import_mssql", return_value=mock_mssql):
            with pytest.raises(ConnectError, match="TrustServerCertificate"):
                db.create_connection()

    def test_non_ssl_error_passes_through(self):
        db = _make_mssql()
        mock_mssql = MagicMock()
        mock_mssql.Error = type("Error", (Exception,), {})
        mock_mssql.connect.side_effect = mock_mssql.Error("Login failed for user")

        with patch("data_diff.databases.mssql.import_mssql", return_value=mock_mssql):
            with pytest.raises(ConnectError, match="Login failed"):
                db.create_connection()
