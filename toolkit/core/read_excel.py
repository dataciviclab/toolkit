from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _build_excel_params(read_cfg: dict[str, Any]) -> str:
    """Build DuckDB ``read_xlsx`` named parameter string from ``read_cfg``."""
    parts: list[str] = []

    # sheet_name
    sheet = read_cfg.get("sheet_name")
    if sheet is not None:
        if isinstance(sheet, int):
            # read_xlsx vuole VARCHAR — convertiamo in nome default Sheet{N}
            parts.append(f"sheet='Sheet{sheet}'")
        else:
            parts.append(f"sheet='{sheet}'")

    # header
    header = bool(read_cfg.get("header", True))
    parts.append(f"header={'true' if header else 'false'}")

    # skip — gestito via SQL OFFSET, non nativo in read_xlsx
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
        Stringa ``CAST(a AS tipo) AS nuovo, TRIM(b) AS b, ...``
    """
    selects: list[str] = []
    for row in describe:
        name = str(row[0])
        dtype = str(row[1])

        if columns:
            new_name = list(columns.keys())[len(selects)]
            target_type = columns[new_name]
            expr = f"CAST({name} AS {target_type}) AS {new_name}"
        elif trim and "VARCHAR" in dtype.upper():
            expr = f"TRIM({name}) AS {name}"
        else:
            expr = name
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
    if len(input_files) == 1:
        path_str = f"'{input_files[0]}'"
        source = f"read_xlsx({path_str}, {params_str})"
    else:
        paths = ", ".join(f"'{p}'" for p in input_files)
        source = f"read_xlsx([{paths}], {params_str})"

    # Vista base: lettura dal file
    con.execute(f"CREATE OR REPLACE VIEW _raw_base AS SELECT * FROM {source}")

    # Skip rows via SQL OFFSET (read_xlsx non ha skip nativo)
    if skip > 0:
        con.execute(f"CREATE OR REPLACE VIEW _raw_base AS SELECT * FROM _raw_base OFFSET {skip}")

    # Parametri usati (per logging e ReadInfo)
    params_used: dict[str, Any] = {
        "sheet_name": read_cfg.get("sheet_name", 0),
        "header": bool(read_cfg.get("header", True)),
        "skip": skip,
        "trim_whitespace": bool(read_cfg.get("trim_whitespace", True)),
    }

    # Applica mapping colonne e/o trim in un unico passo (evita ricorsione viste)
    columns_cfg = read_cfg.get("columns")
    trim_enabled = read_cfg.get("trim_whitespace", True)

    if columns_cfg or trim_enabled:
        describe = con.execute("DESCRIBE _raw_base").fetchall()
        if columns_cfg:
            new_names = list(columns_cfg.keys())
            if len(new_names) != len(describe):
                raise ValueError(
                    f"Excel input columns mismatch. "
                    f"Configured={len(new_names)} detected={len(describe)}"
                )
        select_expr = _build_select_expr(describe, columns=columns_cfg, trim=trim_enabled)
        con.execute(f"CREATE OR REPLACE VIEW raw_input AS SELECT {select_expr} FROM _raw_base")
    else:
        con.execute("CREATE OR REPLACE VIEW raw_input AS SELECT * FROM _raw_base")

    # Log parametri
    if columns_cfg:
        params_used["columns"] = dict(columns_cfg)

    logger.info(
        "read_excel params used: source=excel params=%s",
        json.dumps(params_used, ensure_ascii=False, sort_keys=True),
    )
    return {"source": "excel", "params_used": params_used}
