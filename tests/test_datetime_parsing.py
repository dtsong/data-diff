import sys
from datetime import datetime, timezone

import pytest

from data_diff.databases.base import _parse_datetime


class TestParseDatetime:
    def test_standard_microsecond_format(self):
        result = _parse_datetime("2022-06-03 12:24:35.123456")
        assert result == datetime(2022, 6, 3, 12, 24, 35, 123456)

    def test_millisecond_precision(self):
        result = _parse_datetime("2022-06-03 12:24:35.123")
        assert result == datetime(2022, 6, 3, 12, 24, 35, 123000)

    def test_no_fractional_seconds(self):
        result = _parse_datetime("2022-06-03 12:24:35")
        assert result == datetime(2022, 6, 3, 12, 24, 35)

    def test_nanosecond_precision_truncated(self):
        result = _parse_datetime("2022-06-03 12:24:35.123456789")
        assert result == datetime(2022, 6, 3, 12, 24, 35, 123456)

    def test_trailing_whitespace(self):
        result = _parse_datetime("2022-06-03 12:24:35.123456 ")
        assert result == datetime(2022, 6, 3, 12, 24, 35, 123456)

    @pytest.mark.skipif(sys.version_info < (3, 11), reason="fromisoformat timezone support requires 3.11+")
    def test_timezone_suffix(self):
        result = _parse_datetime("2022-06-03T12:24:35+00:00")
        assert result == datetime(2022, 6, 3, 12, 24, 35, tzinfo=timezone.utc)

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError):
            _parse_datetime("not-a-date")
