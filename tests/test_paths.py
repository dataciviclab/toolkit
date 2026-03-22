from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

from toolkit.core.paths import from_root_relative, layer_year_dir, resolve_root, to_root_relative


def test_resolve_root_returns_expanded_explicit_path(tmp_path):
    root = resolve_root(tmp_path / "out")
    assert root == (tmp_path / "out").resolve()


def test_resolve_root_canonicalizes_relative_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = resolve_root("out")
    assert root == (tmp_path / "out").resolve()


def test_resolve_root_requires_explicit_value():
    with pytest.raises(TypeError):
        resolve_root(None)  # type: ignore[arg-type]


def test_layer_year_dir_with_explicit_root(tmp_path):
    p = layer_year_dir(tmp_path, "clean", "x", 2023)
    assert p == tmp_path / "data" / "clean" / "x" / "2023"


def test_to_root_relative_uses_forward_slashes_for_posix_paths():
    root = PurePosixPath("/repo/out")
    path = PurePosixPath("/repo/out/data/raw/demo/2022/file.csv")

    assert to_root_relative(path, root) == "data/raw/demo/2022/file.csv"


def test_to_root_relative_uses_forward_slashes_for_windows_like_paths():
    root = PureWindowsPath(r"C:\repo\out")
    path = PureWindowsPath(r"C:\repo\out\data\raw\demo\2022\file.csv")

    assert to_root_relative(path, root) == "data/raw/demo/2022/file.csv"


def test_from_root_relative_round_trips_posix_relative_path():
    root = PurePosixPath("/repo/out")
    rel = "data/raw/demo/2022/file.csv"

    assert from_root_relative(rel, root) == Path("/repo/out/data/raw/demo/2022/file.csv")


def test_from_root_relative_accepts_forward_slashes_for_windows_like_root():
    root = PureWindowsPath(r"C:\repo\out")
    rel = "data/raw/demo/2022/file.csv"

    assert str(from_root_relative(rel, root)) == r"C:\repo\out\data\raw\demo\2022\file.csv"
