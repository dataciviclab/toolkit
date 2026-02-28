from pathlib import Path

from toolkit.core.manifest import read_manifest
from toolkit.raw.run import _infer_ext, run_raw


class _NoopLogger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None

    def error(self, *_args, **_kwargs):
        return None


def test_infer_ext_http_csv_php_and_zip_php():
    assert _infer_ext("http_file", {"url": "https://example.org/dataset.csv.php"}) == ".csv"
    assert _infer_ext("http_file", {"url": "https://example.org/archive.zip.php"}) == ".zip"


def test_infer_ext_never_returns_php():
    assert _infer_ext("http_file", {"url": "https://example.org/download.php?id=42"}) != ".php"
    assert _infer_ext("local_file", {"path": "C:/tmp/file.php"}) != ".php"


def test_run_raw_filename_override_has_priority(monkeypatch, tmp_path: Path):
    def _fake_fetch_payload(_stype: str, _client: dict, _formatted_args: dict):
        return b"a,b\n1,2\n", "https://example.org/dataset.csv.php"

    monkeypatch.setattr("toolkit.raw.run._fetch_payload", _fake_fetch_payload)

    raw_cfg = {
        "source": {
            "name": "my_source",
            "type": "http_file",
            "args": {
                "url": "https://example.org/dataset.csv.php",
                "filename": "forced_name.data",
            },
        }
    }

    run_raw("demo", 2024, str(tmp_path), raw_cfg, _NoopLogger())

    out_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    assert (out_dir / "forced_name.data").exists()
    assert not any(p.suffix == ".php" for p in out_dir.iterdir())


def test_run_raw_avoids_overwrite_with_incremental_suffix(monkeypatch, tmp_path: Path):
    def _fake_fetch_payload(_stype: str, _client: dict, _formatted_args: dict):
        return b"new-content\n", "https://example.org/file.csv"

    monkeypatch.setattr("toolkit.raw.run._fetch_payload", _fake_fetch_payload)

    out_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    out_dir.mkdir(parents=True, exist_ok=True)
    existing = out_dir / "file.csv"
    existing.write_bytes(b"old-content\n")

    raw_cfg = {
        "source": {
            "name": "my_source",
            "type": "http_file",
            "args": {"url": "https://example.org/file.csv", "filename": "file.csv"},
        }
    }

    run_raw("demo", 2024, str(tmp_path), raw_cfg, _NoopLogger())

    assert existing.read_bytes() == b"old-content\n"
    assert (out_dir / "file_1.csv").exists()
    assert (out_dir / "file_1.csv").read_bytes() == b"new-content\n"


def test_manifest_created(monkeypatch, tmp_path: Path):
    def _fake_fetch_payload(_stype: str, _client: dict, _formatted_args: dict):
        return b"a,b\n1,2\n", "https://example.org/manifest.csv"

    monkeypatch.setattr("toolkit.raw.run._fetch_payload", _fake_fetch_payload)

    raw_cfg = {
        "source": {
            "name": "primary_source",
            "type": "http_file",
            "args": {"url": "https://example.org/manifest.csv", "filename": "manifest.csv"},
        }
    }

    run_raw("demo", 2024, str(tmp_path), raw_cfg, _NoopLogger(), run_id="run-123")

    out_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    manifest = read_manifest(out_dir)
    assert manifest is not None
    assert manifest["dataset"] == "demo"
    assert manifest["year"] == 2024
    assert manifest["run_id"] == "run-123"
    assert isinstance(manifest["created_at"], str)
    assert manifest["sources"] == [{"name": "primary_source", "output_file": "manifest.csv"}]
    assert manifest["primary_output_file"] == "manifest.csv"


def test_manifest_points_to_latest_in_versioned(monkeypatch, tmp_path: Path):
    payloads = iter([b"old\n", b"new\n"])

    def _fake_fetch_payload(_stype: str, _client: dict, _formatted_args: dict):
        return next(payloads), "https://example.org/file.csv"

    monkeypatch.setattr("toolkit.raw.run._fetch_payload", _fake_fetch_payload)

    raw_cfg = {
        "source": {
            "name": "my_source",
            "type": "http_file",
            "args": {"url": "https://example.org/file.csv", "filename": "file.csv"},
        }
    }

    run_raw("demo", 2024, str(tmp_path), raw_cfg, _NoopLogger(), run_id="run-1")
    run_raw("demo", 2024, str(tmp_path), raw_cfg, _NoopLogger(), run_id="run-2")

    out_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    manifest = read_manifest(out_dir)
    assert manifest is not None
    assert manifest["run_id"] == "run-2"
    assert manifest["primary_output_file"] == "file_1.csv"
    assert (out_dir / "file.csv").read_bytes() == b"old\n"
    assert (out_dir / "file_1.csv").read_bytes() == b"new\n"


def test_manifest_overwrite_policy(monkeypatch, tmp_path: Path):
    payloads = iter([b"old\n", b"new\n"])

    def _fake_fetch_payload(_stype: str, _client: dict, _formatted_args: dict):
        return next(payloads), "https://example.org/file.csv"

    monkeypatch.setattr("toolkit.raw.run._fetch_payload", _fake_fetch_payload)

    raw_cfg = {
        "output_policy": "overwrite",
        "source": {
            "name": "my_source",
            "type": "http_file",
            "args": {"url": "https://example.org/file.csv", "filename": "file.csv"},
        },
    }

    run_raw("demo", 2024, str(tmp_path), raw_cfg, _NoopLogger(), run_id="run-1")
    run_raw("demo", 2024, str(tmp_path), raw_cfg, _NoopLogger(), run_id="run-2")

    out_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    manifest = read_manifest(out_dir)
    assert manifest is not None
    assert manifest["run_id"] == "run-2"
    assert manifest["primary_output_file"] == "file.csv"
    assert (out_dir / "file.csv").read_bytes() == b"new\n"
    assert not (out_dir / "file_1.csv").exists()


def test_multisource_primary_selection(monkeypatch, tmp_path: Path):
    def _fake_fetch_payload(_stype: str, _client: dict, formatted_args: dict):
        return f"payload:{formatted_args['filename']}\n".encode("utf-8"), formatted_args["filename"]

    monkeypatch.setattr("toolkit.raw.run._fetch_payload", _fake_fetch_payload)

    raw_cfg = {
        "sources": [
            {
                "name": "alpha",
                "type": "http_file",
                "args": {"url": "https://example.org/a.csv", "filename": "a.csv"},
            },
            {
                "name": "beta",
                "type": "http_file",
                "primary": True,
                "args": {"url": "https://example.org/b.csv", "filename": "b.csv"},
            },
        ]
    }

    run_raw("demo", 2024, str(tmp_path), raw_cfg, _NoopLogger(), run_id="run-123")

    out_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    manifest = read_manifest(out_dir)
    assert manifest is not None
    assert manifest["sources"] == [
        {"name": "alpha", "output_file": "a.csv"},
        {"name": "beta", "output_file": "b.csv"},
    ]
    assert manifest["primary_output_file"] == "b.csv"
