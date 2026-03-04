import json
from pathlib import Path
import shutil
import re

import duckdb

from toolkit.clean.run import run_clean
from toolkit.cli.cmd_validate import validate as validate_cmd
from toolkit.core.config import load_config
from toolkit.mart.run import run_mart
from toolkit.raw.run import run_raw


class _NoopLogger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None

    def error(self, *_args, **_kwargs):
        return None


def _assert_file_can_be_replaced(path: Path) -> None:
    original = path.read_bytes()
    path.unlink()
    path.write_bytes(original)
    assert path.exists()


def _assert_no_absolute_paths_in_json_payload(payload: dict, root: Path) -> None:
    serialized = json.dumps(payload, ensure_ascii=False)
    assert not re.search(r"[A-Za-z]:\\\\", serialized)
    assert '": "/' not in serialized
    assert str(root.resolve()) not in serialized


def test_project_example_golden_path(tmp_path: Path, monkeypatch):
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)

    monkeypatch.chdir(dst)
    cfg = load_config(dst / "dataset.yml")
    year = cfg.years[0]
    logger = _NoopLogger()

    run_raw(
        cfg.dataset,
        year,
        cfg.root,
        cfg.raw,
        logger,
        base_dir=cfg.base_dir,
        output_cfg=cfg.output,
        clean_cfg=cfg.clean,
    )
    run_clean(cfg.dataset, year, cfg.root, cfg.clean, logger, base_dir=cfg.base_dir, output_cfg=cfg.output)
    run_mart(cfg.dataset, year, cfg.root, cfg.mart, logger, base_dir=cfg.base_dir, output_cfg=cfg.output)
    validate_cmd(step="clean", config=str(dst / "dataset.yml"))
    validate_cmd(step="mart", config=str(dst / "dataset.yml"))

    root = Path(cfg.root)
    raw_dir = root / "data" / "raw" / cfg.dataset / str(year)
    clean_dir = root / "data" / "clean" / cfg.dataset / str(year)
    mart_dir = root / "data" / "mart" / cfg.dataset / str(year)
    clean_parquet = clean_dir / f"{cfg.dataset}_{year}_clean.parquet"
    mart_regione = mart_dir / "rd_by_regione.parquet"
    mart_provincia = mart_dir / "rd_by_provincia.parquet"

    assert (raw_dir / "raw_validation.json").exists()
    assert (raw_dir / "metadata.json").exists()
    assert (raw_dir / "manifest.json").exists()
    assert (raw_dir / "_profile" / "suggested_read.yml").exists()
    assert clean_parquet.exists()
    assert (clean_dir / "metadata.json").exists()
    assert (clean_dir / "manifest.json").exists()
    assert (clean_dir / "_validate" / "clean_validation.json").exists()
    assert mart_regione.exists()
    assert mart_provincia.exists()
    assert (mart_dir / "metadata.json").exists()
    assert (mart_dir / "manifest.json").exists()
    assert (mart_dir / "_validate" / "mart_validation.json").exists()

    raw_meta = json.loads((raw_dir / "metadata.json").read_text(encoding="utf-8"))
    clean_meta = json.loads((clean_dir / "metadata.json").read_text(encoding="utf-8"))
    mart_meta = json.loads((mart_dir / "metadata.json").read_text(encoding="utf-8"))
    raw_manifest = json.loads((raw_dir / "manifest.json").read_text(encoding="utf-8"))
    clean_manifest = json.loads((clean_dir / "manifest.json").read_text(encoding="utf-8"))
    mart_manifest = json.loads((mart_dir / "manifest.json").read_text(encoding="utf-8"))

    for meta in (raw_meta, clean_meta, mart_meta):
        assert meta["metadata_schema_version"] == 1
        assert "toolkit_version" in meta
        assert "config_hash" in meta
        assert isinstance(meta["config_hash"], str)
        assert meta["config_hash"]
        assert "inputs" in meta
        assert isinstance(meta["inputs"], list)
        assert meta["inputs"]
        assert "outputs" in meta
        assert isinstance(meta["outputs"], list)
        assert meta["outputs"]
        assert {"file", "sha256", "bytes"} <= set(meta["outputs"][0].keys())
        assert {"file", "sha256", "bytes"} <= set(meta["inputs"][0].keys())

    assert raw_manifest["metadata"] == "metadata.json"
    assert raw_manifest["validation"] == "raw_validation.json"
    assert raw_manifest["summary"]["ok"] is True
    assert isinstance(raw_manifest["summary"]["errors_count"], int)
    assert isinstance(raw_manifest["summary"]["warnings_count"], int)
    assert raw_manifest["primary_output_file"] == raw_manifest["outputs"][0]["file"]
    assert (raw_dir / raw_manifest["primary_output_file"]).exists()
    assert raw_manifest["sources"]
    assert raw_manifest["outputs"]
    assert raw_meta["profile_hints"]["file_used"] == raw_manifest["primary_output_file"]
    assert raw_meta["profile_hints"]["encoding_suggested"] == "utf-8"
    assert raw_meta["profile_hints"]["delim_suggested"] == ";"
    assert raw_meta["profile_hints"]["columns_preview"]
    assert raw_meta["profile_hints"]["columns_preview"][0] == "Regione"
    assert any("Provincia" in column for column in raw_meta["profile_hints"]["columns_preview"])
    assert clean_meta["input_files"] == [Path(raw_manifest["primary_output_file"]).name]
    assert clean_meta["read_source_used"] in {"strict", "robust", "parquet"}
    assert isinstance(clean_meta["read_params_used"], dict)
    assert isinstance(clean_meta["read_params_source"], list)
    assert clean_meta["read_params_source"]
    assert clean_meta["sql"] == "sql/clean.sql"
    assert clean_meta["sql_rendered"] == "data/clean/project_example/2022/_run/clean_rendered.sql"
    assert "debug" not in clean_meta

    _assert_no_absolute_paths_in_json_payload(clean_meta, root)

    assert clean_manifest["metadata"] == "metadata.json"
    assert clean_manifest["validation"] == "_validate/clean_validation.json"
    assert clean_manifest["summary"]["ok"] is True
    assert isinstance(clean_manifest["summary"]["errors_count"], int)
    assert isinstance(clean_manifest["summary"]["warnings_count"], int)
    assert clean_manifest["outputs"]

    assert mart_manifest["metadata"] == "metadata.json"
    assert mart_manifest["validation"] == "_validate/mart_validation.json"
    assert mart_manifest["summary"]["ok"] is True
    assert isinstance(mart_manifest["summary"]["errors_count"], int)
    assert isinstance(mart_manifest["summary"]["warnings_count"], int)
    assert mart_manifest["outputs"]
    assert mart_meta["output_paths"] == [
        "data/mart/project_example/2022/rd_by_regione.parquet",
        "data/mart/project_example/2022/rd_by_provincia.parquet",
    ]
    assert mart_meta["tables"] == [
        {
            "name": "rd_by_regione",
            "sql": "sql/mart/mart_regione_anno.sql",
            "sql_rendered": "data/mart/project_example/2022/_run/01_rd_by_regione_rendered.sql",
            "output": "data/mart/project_example/2022/rd_by_regione.parquet",
        },
        {
            "name": "rd_by_provincia",
            "sql": "sql/mart/mart_provincia_anno.sql",
            "sql_rendered": "data/mart/project_example/2022/_run/02_rd_by_provincia_rendered.sql",
            "output": "data/mart/project_example/2022/rd_by_provincia.parquet",
        },
    ]
    assert "debug" not in mart_meta

    _assert_no_absolute_paths_in_json_payload(mart_meta, root)

    clean_metadata_json = json.loads((clean_dir / "metadata.json").read_text(encoding="utf-8"))
    mart_metadata_json = json.loads((mart_dir / "metadata.json").read_text(encoding="utf-8"))
    _assert_no_absolute_paths_in_json_payload(clean_metadata_json, root)
    _assert_no_absolute_paths_in_json_payload(mart_metadata_json, root)

    con = duckdb.connect(":memory:")
    assert int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{clean_parquet.as_posix()}')").fetchone()[0]) > 0
    assert int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{mart_regione.as_posix()}')").fetchone()[0]) > 0
    assert int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{mart_provincia.as_posix()}')").fetchone()[0]) > 0
    con.close()


def test_project_example_outputs_can_be_replaced_after_run(tmp_path: Path, monkeypatch):
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)

    monkeypatch.chdir(dst)
    cfg = load_config(dst / "dataset.yml")
    year = cfg.years[0]
    logger = _NoopLogger()

    run_raw(
        cfg.dataset,
        year,
        cfg.root,
        cfg.raw,
        logger,
        base_dir=cfg.base_dir,
        output_cfg=cfg.output,
        clean_cfg=cfg.clean,
    )
    run_clean(cfg.dataset, year, cfg.root, cfg.clean, logger, base_dir=cfg.base_dir, output_cfg=cfg.output)
    run_mart(cfg.dataset, year, cfg.root, cfg.mart, logger, base_dir=cfg.base_dir, output_cfg=cfg.output)

    root = Path(cfg.root)
    clean_parquet = root / "data" / "clean" / cfg.dataset / str(year) / f"{cfg.dataset}_{year}_clean.parquet"
    mart_regione = root / "data" / "mart" / cfg.dataset / str(year) / "rd_by_regione.parquet"

    _assert_file_can_be_replaced(clean_parquet)
    _assert_file_can_be_replaced(mart_regione)
