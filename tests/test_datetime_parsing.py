import sys
from datetime import datetime, timedelta, timezone

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

    def test_seven_fractional_digits_truncated(self):
        result = _parse_datetime("2022-06-03 12:24:35.1234567")
        assert result == datetime(2022, 6, 3, 12, 24, 35, 123456)

    def test_trailing_whitespace(self):
        result = _parse_datetime("2022-06-03 12:24:35.123456 ")
        assert result == datetime(2022, 6, 3, 12, 24, 35, 123456)

    def test_leading_whitespace(self):
        result = _parse_datetime("  2022-06-03 12:24:35.123456")
        assert result == datetime(2022, 6, 3, 12, 24, 35, 123456)

    def test_timezone_offset(self):
        result = _parse_datetime("2022-06-03T12:24:35+00:00")
        assert result == datetime(2022, 6, 3, 12, 24, 35, tzinfo=timezone.utc)

    def test_z_suffix_utc(self):
        result = _parse_datetime("2022-06-03T12:24:35Z")
        assert result == datetime(2022, 6, 3, 12, 24, 35, tzinfo=timezone.utc)

    @pytest.mark.skipif(
        sys.version_info < (3, 11), reason="fromisoformat needs 3.11+ for fractional seconds with tz offset"
    )
    def test_nanosecond_precision_with_timezone(self):
        result = _parse_datetime("2022-06-03T12:24:35.123456789+05:30")
        expected_tz = timezone(timedelta(hours=5, minutes=30))
        assert result == datetime(2022, 6, 3, 12, 24, 35, 123456, tzinfo=expected_tz)

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError):
            _parse_datetime("not-a-date")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            _parse_datetime("")
