import pytest

from toolkit.core.template import render_template, _strip_sql_comments

pytestmark = pytest.mark.pure_unit


def test_render_template_raises_clear_error_for_unresolved_placeholder():
    with pytest.raises(ValueError, match=r"unresolved placeholders.*\{root_posix\}"):
        render_template("select * from read_parquet('{root_posix}/file.parquet')", {"year": 2024})


def test_render_template_raises_clear_error_for_unresolved_dotted_placeholder():
    with pytest.raises(ValueError, match=r"unresolved placeholders.*\{support.lookup.mart\}"):
        render_template(
            "select * from read_parquet('{support.lookup.mart}')",
            {"year": 2024},
        )


def test_comment_placeholder_does_not_raise():
    """Placeholders inside SQL comments (-- ...) are ignored."""
    sql = (
        "-- Template contains {n} inside a comment from DuckDB error\n"
        "SELECT {year}::INTEGER AS anno\n"
        "FROM raw_input\n"
    )
    result = render_template(sql, {"year": 2024})
    assert "2024" in result
    assert "{n}" in result  # comment preserved as-is


def test_unresolved_in_code_still_raises():
    """Unresolved placeholders outside comments still raise."""
    sql = "-- comment with {n}\nSELECT * FROM {unknown_placeholder}\n"
    with pytest.raises(ValueError, match=r"\{unknown_placeholder\}"):
        render_template(sql, {"year": 2024})


def test_strip_sql_comments_empty():
    assert _strip_sql_comments("") == ""


def test_strip_sql_comments_only_comments():
    # A single comment line -- the regex removes the text but the newline
    # is not part of the match (multiline $), so we get empty string.
    assert _strip_sql_comments("-- just a comment") == ""


def test_strip_sql_comments_mixed():
    text = "SELECT 1\n-- comment\nFROM raw\n"
    result = _strip_sql_comments(text)
    assert "comment" not in result
    assert "SELECT 1" in result
    assert "FROM raw" in result
