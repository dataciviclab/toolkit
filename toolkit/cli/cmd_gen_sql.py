from __future__ import annotations

from pathlib import Path
from typing import Any

import typer

from toolkit.core.config import load_config
from toolkit.core.logging import get_logger
from toolkit.clean.generator import generate_clean_sql


def gen_sql(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Anno specifico (se omesso: primo anno in dataset.yml)"),
    out_dir: str | None = typer.Option(None, "--out", help="Root progetto dove creare sql/_generated/clean.sql"),
):
    """
    Genera uno skeleton CLEAN SQL da clean.mapping (assist, non pipeline).
    Scrive: <root>/sql/_generated/clean.sql
    """
    cfg = load_config(config)
    logger = get_logger()

    clean_cfg: dict[str, Any] = cfg.clean or {}
    mapping = clean_cfg.get("mapping")
    if not isinstance(mapping, dict) or not mapping:
        raise typer.BadParameter("dataset.yml deve includere clean.mapping (non vuota).")

    derive = clean_cfg.get("derive")
    if derive is not None and not isinstance(derive, dict):
        raise typer.BadParameter("clean.derive deve essere una mappa (dict) oppure omesso.")

    y = int(year) if year is not None else int(list(cfg.years)[0])

    root = Path(out_dir) if out_dir else cfg.base_dir
    target_dir = root / "sql" / "_generated"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / "clean.sql"

    sql_text = generate_clean_sql(
        dataset=cfg.dataset,
        year=y,
        mapping=mapping,
        derive=derive,
    )

    target_path.write_text(sql_text, encoding="utf-8")
    logger.info(f"GEN-SQL -> {target_path}")


def register(app: typer.Typer) -> None:
    app.command("gen-sql")(gen_sql)
