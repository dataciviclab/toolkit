from __future__ import annotations

from pathlib import Path

from toolkit.clean.sql_runner import run_sql
from toolkit.core.metadata import write_metadata
from toolkit.core.paths import layer_year_dir
from toolkit.core.template import render_template

_ALLOWED_INPUT_EXTS = {".csv", ".tsv", ".txt", ".parquet"}


def _list_input_files(raw_dir: Path) -> list[Path]:
    files: list[Path] = []
    for p in raw_dir.iterdir():
        if not p.is_file():
            continue
        if p.suffix.lower() not in _ALLOWED_INPUT_EXTS:
            continue
        # skip empty files
        try:
            if p.stat().st_size == 0:
                continue
        except Exception:
            pass
        files.append(p)
    return sorted(files)


def run_clean(
    dataset: str,
    year: int,
    root: str | None,
    clean_cfg: dict,
    logger,
    *,
    base_dir: Path | None = None,
):
    raw_dir = layer_year_dir(root, "raw", dataset, year)
    out_dir = layer_year_dir(root, "clean", dataset, year)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not raw_dir.exists():
        raise FileNotFoundError(f"RAW dir not found: {raw_dir}. Run: toolkit run raw -c dataset.yml")

    input_files = _list_input_files(raw_dir)
    if not input_files:
        raise FileNotFoundError(
            f"No usable RAW files found in {raw_dir}. "
            f"Expected one of: {sorted(_ALLOWED_INPUT_EXTS)}"
        )

    sql_rel = clean_cfg.get("sql")
    if not sql_rel:
        raise ValueError("clean.sql missing in dataset.yml (expected: clean: { sql: 'sql/clean.sql' })")

    sql_path = Path(sql_rel)
    if base_dir and not sql_path.is_absolute():
        sql_path = base_dir / sql_path
    if not sql_path.exists():
        raise FileNotFoundError(f"CLEAN SQL file not found: {sql_path}")

    sql = sql_path.read_text(encoding="utf-8")

    template_ctx = {"year": year, "dataset": dataset}
    sql = render_template(sql, template_ctx)

    # Save rendered SQL for audit/debug (Lab-friendly)
    run_dir = out_dir / "_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    rendered_sql_path = run_dir / "clean_rendered.sql"
    rendered_sql_path.write_text(sql, encoding="utf-8")

    read_cfg = clean_cfg.get("read") or {}
    if not isinstance(read_cfg, dict):
        raise ValueError("clean.read must be a mapping (dict) in dataset.yml")

    output_path = out_dir / f"{dataset}_{year}_clean.parquet"
    run_sql(input_files, sql, output_path, read_cfg=read_cfg)

    write_metadata(
        out_dir,
        {
            "layer": "clean",
            "dataset": dataset,
            "year": year,
            "sql": str(sql_path),
            "sql_rendered": str(rendered_sql_path),
            "template_ctx": template_ctx,
            "read": read_cfg,
            "input_files": [p.name for p in input_files],
        },
    )
    logger.info(f"CLEAN → {output_path}")