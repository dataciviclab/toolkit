import pytest

from toolkit.core.template import render_template


def test_render_template_raises_clear_error_for_unresolved_placeholder():
    with pytest.raises(ValueError, match=r"unresolved placeholders.*\{root_posix\}"):
        render_template("select * from read_parquet('{root_posix}/file.parquet')", {"year": 2024})
