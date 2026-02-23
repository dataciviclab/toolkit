from pathlib import Path

from toolkit.core.paths import resolve_root, layer_year_dir


def test_resolve_root_local_defaults_to_cwd(monkeypatch):
    # Ensure env var doesn't affect test
    monkeypatch.delenv("DCL_ROOT", raising=False)

    # Local default should be project root (cwd), not ./data
    assert resolve_root(None) == Path.cwd()


def test_layer_year_dir_local_layout(monkeypatch):
    monkeypatch.delenv("DCL_ROOT", raising=False)

    p = layer_year_dir(None, "raw", "demo", 2022)
    assert p == Path.cwd() / "data" / "raw" / "demo" / "2022"


def test_layer_year_dir_with_explicit_root(tmp_path):
    p = layer_year_dir(str(tmp_path), "clean", "x", 2023)
    assert p == tmp_path / "data" / "clean" / "x" / "2023"