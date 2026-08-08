from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

from toolkit.core.paths import from_root_relative, layer_year_dir, resolve_root, to_root_relative


@pytest.mark.policy
def test_resolve_root_returns_expanded_explicit_path(tmp_path):
    root = resolve_root(tmp_path / "out")
    assert root == (tmp_path / "out").resolve()


@pytest.mark.policy
def test_resolve_root_canonicalizes_relative_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = resolve_root("out")
    assert root == (tmp_path / "out").resolve()


@pytest.mark.policy
def test_resolve_root_accepts_none():
    """None resolves to the current working directory."""
    root = resolve_root(None)
    assert root.is_absolute()


@pytest.mark.policy
def test_layer_year_dir_with_explicit_root(tmp_path):
    p = layer_year_dir(tmp_path, "clean", "x", 2023)
    assert p == tmp_path / "data" / "clean" / "x" / "2023"


@pytest.mark.policy
def test_to_root_relative_uses_forward_slashes_for_posix_paths():
    root = PurePosixPath("/repo/out")
    path = PurePosixPath("/repo/out/data/raw/demo/2022/file.csv")

    assert to_root_relative(path, root) == "data/raw/demo/2022/file.csv"


@pytest.mark.policy
def test_to_root_relative_uses_forward_slashes_for_windows_like_paths():
    root = PureWindowsPath(r"C:\repo\out")
    path = PureWindowsPath(r"C:\repo\out\data\raw\demo\2022\file.csv")

    assert to_root_relative(path, root) == "data/raw/demo/2022/file.csv"


@pytest.mark.policy
def test_from_root_relative_round_trips_posix_relative_path():
    root = PurePosixPath("/repo/out")
    rel = "data/raw/demo/2022/file.csv"

    assert from_root_relative(rel, root) == Path("/repo/out/data/raw/demo/2022/file.csv")


@pytest.mark.policy
def test_from_root_relative_accepts_forward_slashes_for_windows_like_root():
    root = PureWindowsPath(r"C:\repo\out")
    rel = "data/raw/demo/2022/file.csv"

    assert str(from_root_relative(rel, root)) == r"C:\repo\out\data\raw\demo\2022\file.csv"


# ---------------------------------------------------------------------------
# Config discovery cross-repo (fusion ADR generalizzata)
# ---------------------------------------------------------------------------


@pytest.mark.contract
def test_resolve_config_path_cross_repo(tmp_path):
    """Lo slug si risolve nei repo migrati (datasets/) e in DI (candidates/)."""
    from toolkit.core.discovery import resolve_config_path

    # Repo migrato: layout flat datasets/
    eu = tmp_path / "eurostat" / "datasets" / "eurostat-crime-nuts3"
    eu.mkdir(parents=True)
    (eu / "dataset.yml").write_text("dataset:\n  name: 'eurostat_crime_nuts3'\n", encoding="utf-8")

    # DI: candidates/
    di = tmp_path / "dataset-incubator" / "candidates" / "anac-bandi-gara"
    di.mkdir(parents=True)
    (di / "dataset.yml").write_text("dataset:\n  name: 'anac_bandi_gara'\n", encoding="utf-8")

    assert (
        resolve_config_path("eurostat-crime-nuts3", workspace=tmp_path)
        == (eu / "dataset.yml").resolve()
    )
    assert (
        resolve_config_path("anac-bandi-gara", workspace=tmp_path) == (di / "dataset.yml").resolve()
    )


@pytest.mark.contract
def test_resolve_config_path_not_found(tmp_path):
    """Slug inesistente → FileNotFoundError con suggerimento."""
    from toolkit.core.discovery import resolve_config_path

    with pytest.raises(FileNotFoundError, match="Nessun dataset trovato"):
        resolve_config_path("slug-inesistente", workspace=tmp_path)
