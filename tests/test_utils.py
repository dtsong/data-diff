import re

from data_diff.__main__ import _remove_passwords_in_dict
from data_diff.utils import (
    columns_added_template,
    columns_removed_template,
    columns_type_changed_template,
    dbt_diff_string_template,
    diff_int_dynamic_color_template,
    match_like,
    match_regexps,
    number_to_human,
    remove_passwords_in_dict,
)

# --- remove_passwords_in_dict ---


def test_remove_passwords_in_dict():
    # Test replacing password value
    d = {"password": "mypassword"}
    remove_passwords_in_dict(d)
    assert d["password"] == "***"

    # Test replacing password in database URL
    d = {"database_url": "mysql://user:mypassword@localhost/db"}
    remove_passwords_in_dict(d, "$$$$")
    assert d["database_url"] == "mysql://user:$$$$@localhost/db"

    # Test replacing motherduck token in database URL
    d = {"database_url": "md:datafold_demo?motherduck_token=jaiwefjoaisdk"}
    remove_passwords_in_dict(d, "$$$$")
    assert d["database_url"] == "md:datafold_demo?motherduck_token=$$$$"

    # Test replacing password in nested dictionary
    d = {"info": {"password": "mypassword"}}
    remove_passwords_in_dict(d, "%%")
    assert d["info"]["password"] == "%%"

    # Test replacing a motherduck token in nested dictionary
    d = {"database1": {"driver": "duckdb", "filepath": "md:datafold_demo?motherduck_token=awieojfaowiejacijobhiwaef"}}
    remove_passwords_in_dict(d, "%%")
    assert d["database1"]["filepath"] == "md:datafold_demo?motherduck_token=%%"


def test__main__remove_passwords_in_dict():
    # Test replacing password value
    d = {"password": "mypassword"}
    _remove_passwords_in_dict(d)
    assert d["password"] == "**********"

    # Test replacing password in database URL
    d = {"database_url": "mysql://user:mypassword@localhost/db"}
    _remove_passwords_in_dict(d)
    assert d["database_url"] == "mysql://user:***@localhost/db"

    # Test replacing motherduck token in database URL
    d = {"database_url": "md:datafold_demo?motherduck_token=jaiwefjoaisdk"}
    _remove_passwords_in_dict(d)
    assert d["database_url"] == "md:datafold_demo?motherduck_token=***"

    # Test replacing password in nested dictionary
    d = {"info": {"password": "mypassword"}}
    _remove_passwords_in_dict(d)
    assert d["info"]["password"] == "**********"

    # Test replacing a motherduck token in nested dictionary
    d = {"database1": {"driver": "duckdb", "filepath": "md:datafold_demo?motherduck_token=awieojfaowiejacijobhiwaef"}}
    _remove_passwords_in_dict(d)
    assert d["database1"]["filepath"] == "md:datafold_demo?motherduck_token=**********"


# --- match_regexps ---


def test_match_regexps():
    def only_results(x):
        return [v for k, v in x]

    # Test with no matches
    regexps = {"a*": 1, "b*": 2}
    s = "c"
    assert only_results(match_regexps(regexps, s)) == []

    # Test with one match
    regexps = {"a*": 1, "b*": 2}
    s = "b"
    assert only_results(match_regexps(regexps, s)) == [2]

    # Test with multiple matches
    regexps = {"abc": 1, "ab*c": 2, "c*": 3}
    s = "abc"
    assert only_results(match_regexps(regexps, s)) == [1, 2]

    # Test with regexp that doesn't match the end of the string
    regexps = {"a*b": 1}
    s = "acb"
    assert only_results(match_regexps(regexps, s)) == []


def test_match_like():
    strs = ["abc", "abcd", "ab", "bcd", "def"]

    # Test exact match
    pattern = "abc"
    result = list(match_like(pattern, strs))
    assert result == ["abc"]

    # Test % match
    pattern = "a%"
    result = list(match_like(pattern, strs))
    assert result == ["abc", "abcd", "ab"]

    # Test ? match
    pattern = "a?c"
    result = list(match_like(pattern, strs))
    assert result == ["abc"]


# --- number_to_human ---


def test_number_to_human():
    # Test basic conversion
    assert number_to_human(1000) == "1k"
    assert number_to_human(1000000) == "1m"
    assert number_to_human(1000000000) == "1b"

    # Test decimal values
    assert number_to_human(1234) == "1k"
    assert number_to_human(12345) == "12k"
    assert number_to_human(123456) == "123k"
    assert number_to_human(1234567) == "1m"
    assert number_to_human(12345678) == "12m"
    assert number_to_human(123456789) == "123m"
    assert number_to_human(1234567890) == "1b"

    # Test negative values
    assert number_to_human(-1000) == "-1k"
    assert number_to_human(-1000000) == "-1m"
    assert number_to_human(-1000000000) == "-1b"


# --- diff_int_dynamic_color_template ---


def test_diff_int_dynamic_color_template_string_input():
    assert diff_int_dynamic_color_template("test_string") == "test_string"


def test_diff_int_dynamic_color_template_positive():
    assert diff_int_dynamic_color_template(10) == "[green]+10[/]"


def test_diff_int_dynamic_color_template_negative():
    assert diff_int_dynamic_color_template(-10) == "[red]-10[/]"


def test_diff_int_dynamic_color_template_zero():
    assert diff_int_dynamic_color_template(0) == "0"


# --- dbt_diff_string_template ---


def test_dbt_diff_string_template():
    expected_output = """
rows       PROD    <>            DEV
---------  ------  ------------  ------------------
Total      10                    20 [[green]+10[/]]
Added              [green]+5[/]
Removed            [red]-2[/]
Different          3
Unchanged          5

columns    # diff values
---------  ---------------
info       values"""

    output = dbt_diff_string_template(
        total_rows_table1=10,
        total_rows_table2=20,
        total_rows_diff=10,
        rows_added=5,
        rows_removed=2,
        rows_updated=3,
        rows_unchanged=5,
        extra_info_dict={"info": "values"},
        extra_info_str="extra info",
    )

    assert output == expected_output


# --- columns template methods ---


def _extract_columns_set(output):
    """Extract quoted words by regex and return as a set."""
    output_list = re.findall(r"'(\w*)'", output)
    return set(output_list)


def test_columns_removed_template():
    output = columns_removed_template({"column1", "column2"})
    assert "[red]Columns removed [-2]:[/]" in output
    assert _extract_columns_set(output) == {"column1", "column2"}


def test_columns_added_template():
    output = columns_added_template({"column1", "column2"})
    assert "[green]Columns added [+2]:" in output
    assert _extract_columns_set(output) == {"column1", "column2"}


def test_columns_type_changed_template():
    output = columns_type_changed_template({"column1", "column2"})
    assert "Type changed [2]: [green]" in output
    assert _extract_columns_set(output) == {"column1", "column2"}
