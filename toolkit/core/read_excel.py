from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _build_excel_params(read_cfg: dict[str, Any]) -> str:
    """Build DuckDB ``read_xlsx`` named parameter string from ``read_cfg``."""
    parts: list[str] = []

    # sheet_name — read_xlsx accetta solo stringhe, non indici
    sheet = read_cfg.get("sheet_name")
    if sheet is not None:
        if isinstance(sheet, int):
            raise ValueError(
                "DuckDB read_xlsx does not support integer sheet indexes. "
                "Use the sheet name (string) instead, e.g. sheet_name='Sheet1'."
                f" Got: {sheet!r}"
            )
        parts.append(f"sheet='{sheet}'")

    # header
    header = bool(read_cfg.get("header", True))
    parts.append(f"header={'true' if header else 'false'}")

    return ", ".join(parts)


def _build_select_expr(
    describe: list[Any],
    columns: dict[str, str] | None = None,
    trim: bool = False,
) -> str:
    """Build SELECT expression list for columns mapping and/or trim.

    Args:
        describe: Risultato di ``DESCRIBE`` (lista di tuple (name, type, ...)).
        columns: Se presente, dict ``{nome_nuovo: tipo_duckdb}`` per rename+cast.
        trim: Se True, applica ``TRIM`` a colonne VARCHAR.

    Returns:
        Stringa ``CAST("a" AS tipo) AS nuovo_nome, TRIM("b") AS "b", ...``
        con identificatori quotati per gestire spazi e caratteri speciali.
    """
    selects: list[str] = []
    for row in describe:
        name = str(row[0])
        qname = f'"{name}"'
        dtype = str(row[1])

        if columns:
            new_name = list(columns.keys())[len(selects)]
            target_type = columns[new_name]
            expr = f"CAST({qname} AS {target_type}) AS {new_name}"
        elif trim and "VARCHAR" in dtype.upper():
            expr = f"TRIM({qname}) AS {qname}"
        else:
            expr = qname
        selects.append(expr)

    return ", ".join(selects)


def _execute_excel_read(
    con,
    input_files: list[Path],
    read_cfg: dict[str, Any],
    *,
    logger,
) -> dict[str, Any]:
    """Legge file Excel con DuckDB nativo e registra vista ``raw_input``.

    Usa ``read_excel()`` di DuckDB (estensione ``excel``) invece di pandas.
    Non supporta il formato ``.xls`` (Excel 97-2003), solo ``.xlsx``.

    Args:
        con: Connessione DuckDB aperta.
        input_files: Lista di path a file ``.xlsx``.
        read_cfg: Configurazione con chiavi ``sheet_name``, ``header``,
            ``skip``, ``columns``, ``trim_whitespace``.
        logger: Logger per info.

    Returns:
        ``{"source": "excel", "params_used": {...}}``.
    """
    # Controllo formato — DuckDB non supporta .xls
    for f in input_files:
        if f.suffix.lower() == ".xls":
            raise ValueError(
                "DuckDB excel extension does not support .xls format. "
                "Convert the file to .xlsx or use a different reader."
            )

    # Installa e carica estensione excel
    con.execute("INSTALL excel; LOAD excel;")

    # Parametri DuckDB read_xlsx
    params_str = _build_excel_params(read_cfg)
    skip = int(read_cfg.get("skip", 0))

    # Lettura con DuckDB read_xlsx
    # Nota: read_xlsx non accetta array di path — usiamo UNION ALL per multi-file
    if len(input_files) == 1:
        path_str = f"'{input_files[0]}'"
        source = f"SELECT * FROM read_xlsx({path_str}, {params_str})"
    else:
        subqueries = []
        for i, p in enumerate(input_files):
            subqueries.append(f"(SELECT * FROM read_xlsx('{p}', {params_str}))")
        source = " UNION ALL ".join(subqueries)

    # Vista base: lettura dal file
    # Usiamo una sequenza di viste con nomi distinti per evitare ricorsione DuckDB
    # quando applichiamo skip / columns / trim
    con.execute(f"CREATE OR REPLACE VIEW _raw_excel AS {source}")

    # Skip rows via SQL OFFSET (read_xlsx non ha skip nativo)
    if skip > 0:
        con.execute(f"CREATE OR REPLACE VIEW _raw_skip AS SELECT * FROM _raw_excel OFFSET {skip}")
        source_view = "_raw_skip"
    else:
        source_view = "_raw_excel"

    # Parametri usati (per logging e ReadInfo)
    params_used: dict[str, Any] = {
        "sheet_name": read_cfg.get("sheet_name", 0),
        "header": bool(read_cfg.get("header", True)),
        "skip": skip,
        "trim_whitespace": bool(read_cfg.get("trim_whitespace", True)),
    }

    # Applica mapping colonne e/o trim in un unico passo
    columns_cfg = read_cfg.get("columns")
    trim_enabled = read_cfg.get("trim_whitespace", True)

    if columns_cfg or trim_enabled:
        describe = con.execute(f"DESCRIBE {source_view}").fetchall()
        if columns_cfg:
            new_names = list(columns_cfg.keys())
            if len(new_names) != len(describe):
                raise ValueError(
                    f"Excel input columns mismatch. "
                    f"Configured={len(new_names)} detected={len(describe)}"
                )
        select_expr = _build_select_expr(describe, columns=columns_cfg, trim=trim_enabled)
        con.execute(f"CREATE OR REPLACE VIEW raw_input AS SELECT {select_expr} FROM {source_view}")
    else:
        con.execute(f"CREATE OR REPLACE VIEW raw_input AS SELECT * FROM {source_view}")

    # Log parametri
    if columns_cfg:
        params_used["columns"] = dict(columns_cfg)

    logger.info(
        "read_excel params used: source=excel params=%s",
        json.dumps(params_used, ensure_ascii=False, sort_keys=True),
    )
    return {"source": "excel", "params_used": params_used}
