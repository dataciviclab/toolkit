from toolkit.clean import run_clean, run_clean_validation, validate_clean
from toolkit.plugins import CkanSource, HttpFileSource, LocalFileSource, SdmxSource
from toolkit.profile import (
    RawProfile,
    build_profile_hints,
    build_suggested_read_cfg,
    profile_raw,
    write_suggested_read_yml,
)
from toolkit.raw import run_raw, run_raw_validation, validate_raw_output


def test_clean_exports() -> None:
    assert callable(run_clean)
    assert callable(validate_clean)
    assert callable(run_clean_validation)


def test_raw_exports() -> None:
    assert callable(run_raw)
    assert callable(validate_raw_output)
    assert callable(run_raw_validation)


def test_plugins_exports() -> None:
    assert LocalFileSource.__name__ == "LocalFileSource"
    assert HttpFileSource.__name__ == "HttpFileSource"
    assert CkanSource.__name__ == "CkanSource"
    assert SdmxSource.__name__ == "SdmxSource"


def test_profile_exports() -> None:
    assert RawProfile.__name__ == "RawProfile"
    assert callable(profile_raw)
    assert callable(build_profile_hints)
    assert callable(build_suggested_read_cfg)
    assert callable(write_suggested_read_yml)
