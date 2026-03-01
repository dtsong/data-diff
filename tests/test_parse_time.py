from datetime import timedelta

import pytest

from data_diff.parse_time import parse_time_delta


def test_times():
    td = parse_time_delta("1w2d3h4min5s")
    assert td == timedelta(weeks=1, days=2, hours=3, minutes=4, seconds=5)

    assert parse_time_delta("1y") == timedelta(days=365)
    assert parse_time_delta("1mon") == timedelta(days=30)

    with pytest.raises(ValueError):
        parse_time_delta("")

    with pytest.raises(ValueError):
        parse_time_delta("1y1year")

    with pytest.raises(ValueError):
        parse_time_delta("1x")
