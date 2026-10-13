from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from toolkit.core.config import load_config
from toolkit.core.paths import layer_year_dir

# Tipi di support (ADR-005): dataset, codelist, file, external.
SUPPORT_TYPES = ("dataset", "codelist", "file", "external")

# Convenzione layer materializzazione codelist: out/data/support/{name}/{name}.parquet
SUPPORT_LAYER = "support"


def _support_expected_mart_outputs(cfg, year: int) -> list[Path]:
    table_names = [t.name for t in cfg.mart.tables if t.name]
    mart_dir = layer_year_dir(cfg.root, "mart", cfg.dataset, year)
    return [mart_dir / f"{name}.parquet" for name in table_names]


def _support_expected_clean_output(cfg, year: int) -> Path:
    """Parquet CLEAN del support dataset (ADR-005: output attesi = clean + mart)."""
    clean_dir = layer_year_dir(cfg.root, "clean", cfg.dataset, year)
    return clean_dir / f"{cfg.dataset}_{year}_clean.parquet"


def _codelist_output_path(root: Path, name: str) -> Path:
    """Parquet canonico di una codelist materializzata (ADR-005)."""
    return root / "data" / SUPPORT_LAYER / name / f"{name}.parquet"


def _entry_type(entry: dict[str, Any]) -> str:
    stype = str(entry.get("type") or "dataset")
    if stype not in SUPPORT_TYPES:
        raise ValueError(
            f"support[].type must be one of {SUPPORT_TYPES}, got {stype!r} "
            f"for support '{entry.get('name')}'"
        )
    return stype


def resolve_support_payloads(
    support_entries: list[dict[str, Any]] | None,
    *,
    require_exists: bool,
    smoke: bool = False,
    root: Path | None = None,
) -> list[dict[str, Any]]:
    """Risolve gli output attesi di ogni support entry, per tipo (ADR-005).

    - ``type: dataset``: carica il config del support e risolve clean + tutte
      le tabelle mart per anno (comportamento storico esteso al clean).
    - ``type: codelist``: parquet canonico in ``{root}/data/support/{name}/``
      (richiede ``root`` = root del candidate).
    - ``type: file``: path dichiarato (relativo normalizzato sul root).
    - ``type: external``: artifact remoto già pubblicato. Forme supportate:
      ``uri`` pieno, ``repo``+``path`` (GitHub raw), ``repo``+``slug``+``layer``
      (risoluzione via registry produttore), o ``bucket``+``pattern``+``slug``
      (path canonici lab-connectors). Se un config contiene più forme,
      ``uri`` vince (backward-compat esplicita).

    Con ``require_exists=True`` gli output mancanti sollevano errore esplicito
    (mart/clean del support non ancora eseguito, o codelist/file non
    materializzati).
    """
    resolved: list[dict[str, Any]] = []
    for entry in support_entries or []:
        name = str(entry["name"])
        stype = _entry_type(entry)

        if stype == "dataset":
            resolved.append(_resolve_dataset_entry(entry, name, require_exists, smoke))
        elif stype == "codelist":
            resolved.append(_resolve_codelist_entry(entry, name, require_exists, root))
        elif stype == "external":
            resolved.append(_resolve_external_entry(entry, name, require_exists))
        else:  # file
            resolved.append(_resolve_file_entry(entry, name, require_exists, root))
    return resolved


def _resolve_dataset_entry(
    entry: dict[str, Any], name: str, require_exists: bool, smoke: bool
) -> dict[str, Any]:
    if "config" not in entry:
        raise ValueError(f"support dataset '{name}' requires 'config'")
    config_path = Path(entry["config"])
    years = [int(year) for year in entry.get("years") or []]
    if smoke:
        _sup0 = load_config(config_path)
        support_cfg = load_config(config_path, root_override=_sup0.root / "smoke")
    else:
        support_cfg = load_config(config_path)

    year_payloads: list[dict[str, Any]] = []
    all_outputs: list[str] = []
    for year in years:
        mart_paths = _support_expected_mart_outputs(support_cfg, year)
        clean_path = _support_expected_clean_output(support_cfg, year)
        expected_paths = [clean_path, *mart_paths]
        output_paths = [str(path) for path in expected_paths]
        existing_paths = [str(path) for path in expected_paths if path.exists()]
        all_outputs_exist = len(output_paths) > 0 and len(existing_paths) == len(output_paths)
        if require_exists and not mart_paths:
            raise ValueError(
                "Support dataset MART non configurato: "
                f"{name} ({config_path}) anno {year}. "
                "Il dataset di supporto deve dichiarare almeno una tabella in mart.tables."
            )
        if require_exists and not all_outputs_exist:
            missing = [p for p in expected_paths if not p.exists()]
            raise FileNotFoundError(
                "Support dataset output mancante: "
                f"{name} ({config_path}) anno {year}: {missing[0]}. "
                "Esegui prima il run del support dataset o correggi support[].years."
            )
        year_payloads.append(
            {
                "year": year,
                "dataset": support_cfg.dataset,
                "config_path": str(config_path),
                "mart_dir": str(
                    layer_year_dir(support_cfg.root, "mart", support_cfg.dataset, year)
                ),
                "clean": str(clean_path),
                "outputs": output_paths,
                "existing_outputs": existing_paths,
                "all_outputs_exist": all_outputs_exist,
            }
        )
        all_outputs.extend(existing_paths if require_exists else output_paths)

    mart_by_table: dict[str, str] = {}
    for table in support_cfg.mart.tables or []:
        if not table.name:
            continue
        year = years[0] if years else 0
        mart_dir = layer_year_dir(support_cfg.root, "mart", support_cfg.dataset, year)
        mart_by_table[table.name] = str(mart_dir / f"{table.name}.parquet")

    # Build clean glob pattern to include all years (not just the first)
    # when the support dataset has multiple years configured.
    clean_value: str | None = year_payloads[0]["clean"] if year_payloads else None
    if clean_value and len(years) > 1:
        first_clean = Path(clean_value)
        clean_value = str(
            first_clean.parent.parent / "*" / f"{support_cfg.dataset}_*_clean.parquet"
        )

    return {
        "name": name,
        "type": "dataset",
        "config_path": str(config_path),
        "dataset": support_cfg.dataset,
        "years": years,
        "years_resolved": year_payloads,
        "outputs": all_outputs,
        "mart": (str(mart_paths[0]) if mart_paths else None),
        "mart_by_table": mart_by_table,
        "clean": clean_value,
        "path": None,
        "all_outputs_exist": all(yp["all_outputs_exist"] for yp in year_payloads)
        if year_payloads
        else False,
    }


def _resolve_codelist_entry(
    entry: dict[str, Any], name: str, require_exists: bool, root: Path | None
) -> dict[str, Any]:
    if root is None:
        raise ValueError(f"support codelist '{name}' requires the candidate root")
    if not entry.get("id"):
        raise ValueError(f"support codelist '{name}' requires 'id'")
    target = _codelist_output_path(root, name)
    all_outputs_exist = target.exists()
    if require_exists and not all_outputs_exist:
        raise FileNotFoundError(
            f"Support codelist '{name}' non materializzata: {target}. "
            "Esegui il run del candidate (materializza prima) o --refresh-support."
        )
    return {
        "name": name,
        "type": "codelist",
        "config_path": None,
        "dataset": None,
        "years": [],
        "years_resolved": [],
        "outputs": [str(target)],
        "existing_outputs": [str(target)] if all_outputs_exist else [],
        "all_outputs_exist": all_outputs_exist,
        "mart": None,
        "mart_by_table": {},
        "clean": None,
        "path": str(target),
    }


def _resolve_file_entry(
    entry: dict[str, Any], name: str, require_exists: bool, root: Path | None
) -> dict[str, Any]:
    if not entry.get("path"):
        raise ValueError(f"support file '{name}' requires 'path'")
    if root is None:
        raise ValueError(f"support file '{name}' requires the candidate root")
    raw_path = Path(entry["path"])
    target = raw_path if raw_path.is_absolute() else Path(root) / raw_path
    all_outputs_exist = target.exists()
    if require_exists and not all_outputs_exist:
        raise FileNotFoundError(
            f"Support file '{name}' mancante: {target}. "
            "Esegui il run del candidate (materializza prima) o --refresh-support."
        )
    return {
        "name": name,
        "type": "file",
        "config_path": None,
        "dataset": None,
        "years": [],
        "years_resolved": [],
        "outputs": [str(target)],
        "existing_outputs": [str(target)] if all_outputs_exist else [],
        "all_outputs_exist": all_outputs_exist,
        "mart": None,
        "mart_by_table": {},
        "clean": None,
        "path": str(target),
    }


def _resolve_external_entry(
    entry: dict[str, Any], name: str, require_exists: bool
) -> dict[str, Any]:
    """Resolve a GCS-hosted parquet file via lab_connectors path patterns.

    Ordine di risoluzione del template URI (vedi ``_external_uri_template``):

    1. ``uri``: full gs:// or https URL (vince se presente — backward-compat).
       May contain ``{year}``.
    2. ``repo`` + ``path``: file raw nel repo su GitHub
       (``raw.githubusercontent.com/dataciviclab/{repo}/main/{path}``).
    3. ``repo`` + ``slug`` + ``layer`` (``clean``|``mart``): risolve il prefix
       e il pattern dal ``registry/registry.json`` del repo produttore
       (locale workspace → fallback GitHub). Opzionale: ``table`` (mart),
       ``years``, ``registry`` (path esplicito al registry).
    4. ``bucket`` + ``pattern`` + ``slug`` (+ optional ``table``/``prefix``):
       built via ``lab_connectors.gcs.paths.https_url()``. Con ``years`` multi
       il template conserva ``{year}`` (equivale al modo ``uri``+``{year}``).

    Resolution after a URI template is built:
    - ``{year}`` placeholder + ``years`` → one URL per year; ``clean`` is a
      list (DuckDB read_parquet accepts lists; globs don't work over HTTP).
    - no ``{year}`` → single file, ``years`` ignored.
    """
    uri = _external_uri_template(entry, name)
    years = [int(y) for y in entry.get("years") or []]
    has_year_placeholder = "{year}" in uri

    # Multi-year: resolve per year when uri contains {year}
    if has_year_placeholder and years:
        resolved_uris = [uri.replace("{year}", str(y)) for y in years]
        # clean = lista URL risolti (DuckDB read_parquet accetta liste; i glob
        # non funzionano su HTTP → lista è l'unico formato sicuro per external)
        return {
            "name": name,
            "type": "external",
            "config_path": None,
            "dataset": None,
            "years": years,
            "years_resolved": [{"year": y, "path": u} for y, u in zip(years, resolved_uris)],
            "outputs": resolved_uris,
            "existing_outputs": resolved_uris,
            "all_outputs_exist": True,
            "mart": None,
            "mart_by_table": {},
            "clean": resolved_uris,  # lista URL per anno (read_parquet accetta liste)
            "path": uri,  # template URI per {support.X.path} (single-anno)
        }

    # Single file (no {year} in uri, or no years configured)
    return {
        "name": name,
        "type": "external",
        "config_path": None,
        "dataset": None,
        "years": years,
        "years_resolved": [],
        "outputs": [uri],
        "existing_outputs": [uri],
        "all_outputs_exist": True,
        "mart": None,
        "mart_by_table": {},
        "clean": None,
        "path": uri,
    }


def _external_uri_template(entry: dict[str, Any], name: str) -> str:
    """Ritorna il template URI di un support external.

    Ordine di risoluzione (prima vince): ``uri`` esplicito (backward-compat)
    → ``repo``+``path`` (GitHub raw) → ``repo``+``slug`` (registry produttore)
    → ``bucket``+``pattern``+``slug``.
    """
    uri = entry.get("uri")
    if uri:
        return str(uri)

    repo = entry.get("repo")
    if repo:
        raw_path = entry.get("path")
        if raw_path:
            return _external_github_raw_uri(str(repo), str(raw_path))
        slug = entry.get("slug")
        if slug:
            return _external_registry_uri_template(entry, name)

    bucket = entry.get("bucket")
    pattern = entry.get("pattern")
    slug = entry.get("slug")
    if bucket and pattern and slug:
        try:
            from lab_connectors.gcs.paths import https_url

            kwargs: dict[str, Any] = {"slug": slug}
            if entry.get("table"):
                kwargs["table"] = entry["table"]
            years = [int(y) for y in entry.get("years") or []]
            if len(years) > 1:
                # Multi-year: il template conserva {year} (≡ uri+{year});
                # _resolve_external_entry espande una URL per anno.
                kwargs["year"] = "{year}"
            elif years:
                kwargs["year"] = years[0]
            if entry.get("prefix"):
                kwargs["prefix"] = entry["prefix"]
            return https_url(bucket, pattern, **kwargs)
        except ImportError:
            raise ValueError(
                f"support external '{name}' requires lab-connectors with gcs support. "
                "Install: pip install 'lab-connectors[gcs]'"
            )

    raise ValueError(
        f"support external '{name}' requires one of: "
        "'repo'+'slug'+'layer' (registry), 'repo'+'path' (GitHub raw), "
        "'uri', or 'bucket'+'pattern'+'slug'"
    )


def _external_github_raw_uri(repo: str, path: str) -> str:
    """URI GitHub raw per un file del repo produttore (branch main)."""
    rel = path.lstrip("/")
    return f"https://raw.githubusercontent.com/dataciviclab/{repo}/main/{rel}"


def _find_local_registry_path(repo: str) -> Path | None:
    """Cerca ``{repo}/registry/registry.json`` nei repo dati del workspace."""
    from toolkit.core import paths as core_paths
    from toolkit.registry.layout import workspace_repo_dirs

    for repo_dir in workspace_repo_dirs(core_paths.WORKSPACE_ROOT):
        if repo_dir.name == repo:
            candidate = repo_dir / "registry" / "registry.json"
            if candidate.is_file():
                return candidate
    return None


def _load_producer_registry(repo: str, *, registry_path: str | None = None):
    """Carica il registry del repo produttore: path esplicito → locale → GitHub."""
    from lab_connectors.registry.client import load_registry_github, load_registry_local

    if registry_path:
        return load_registry_local(Path(registry_path))

    local = _find_local_registry_path(repo)
    if local is not None:
        return load_registry_local(local)

    try:
        return load_registry_github(repo)
    except Exception as exc:
        local_hint = f"né un registry locale per '{repo}' nel workspace"
        raise ValueError(
            f"support external registry non disponibile per repo '{repo}': "
            f"{local_hint} né su GitHub (dataciviclab/{repo}). "
            "Committa {repo}/registry/registry.json, passa 'registry: <path>', "
            "oppure usa la forma 'repo'+'path' per file GitHub raw."
        ) from exc


def _external_registry_uri_template(entry: dict[str, Any], name: str) -> str:
    """Costruisce il template URI da repo+slug+layer usando il registry produttore.

    Il registry è il contratto cross-repo: slug, prefix GCS e (per i mart)
    le tabelle pubblicate. Un typo di slug fallisce qui con un errore leggibile
    invece di un SQL runtime.
    """
    repo = str(entry["repo"])
    slug = str(entry["slug"])
    layer = str(entry.get("layer") or "clean")
    if layer not in ("clean", "mart"):
        raise ValueError(
            f"support external '{name}': layer must be 'clean' or 'mart', got {layer!r}"
        )

    reg = _load_producer_registry(repo, registry_path=entry.get("registry"))

    from lab_connectors.gcs.paths import https_url

    if layer == "clean":
        hit = next((ds for ds in reg.datasets if ds.slug == slug), None)
        if hit is None:
            available = ", ".join(ds.slug for ds in reg.datasets) or "(vuoto)"
            raise ValueError(
                f"support external '{name}': slug '{slug}' non presente "
                f"nel registry di '{repo}'. Slug disponibili: {available}"
            )
        prefix = reg.prefix_for_slug(slug)
        return https_url("clean", "clean_parquet", slug=slug, prefix=prefix, year="{year}")

    # layer == mart
    table = entry.get("table")
    if not table:
        hits = [m for m in reg.marts if m.dataset == slug]
        if len(hits) == 1:
            table = hits[0].table
        elif not hits:
            raise ValueError(
                f"support external '{name}': nessuna tabella mart per dataset "
                f"'{slug}' nel registry di '{repo}'. Dichiarare 'table:'."
            )
        else:
            tables = ", ".join(m.table for m in hits)
            raise ValueError(
                f"support external '{name}': dataset '{slug}' ha più tabelle mart "
                f"nel registry di '{repo}' ({tables}). Dichiarare 'table:'."
            )
    prefix = _mart_prefix_for_slug(reg, slug)
    return https_url("mart", "mart_parquet", slug=slug, table=table, prefix=prefix, year="{year}")


def _mart_prefix_for_slug(reg: Any, slug: str) -> str:
    """Prefix GCS di un dataset mart: dal dataset registrato, altrimenti dalla mart location."""
    prefix = reg.prefix_for_slug(slug)
    if prefix:
        return prefix
    for m in reg.marts:
        if m.dataset != slug:
            continue
        path = m.location.path
        if not path.startswith("gs://"):
            continue
        marker = f"/{slug}/"
        idx = path.find(marker)
        if idx < 0:
            continue
        first_slash = path.index("/", 5)
        return path[first_slash + 1 : idx + 1]
    return ""


def check_support_path_drift(
    raw_sql: str,
    support_payloads: list[dict[str, Any]],
    sql_label: str = "",
) -> list[str]:
    """
    Scans raw SQL (pre-template-render) for hardcoded path references to
    support datasets that bypass the ``{support.NAME.*}`` placeholder
    contract (mart, clean, mart.TABLE, path, outputs).

    For each declared support entry, verifies that if the support dataset
    slug appears in a path-like string literal, a ``{support.NAME.*}``
    placeholder is also present in the SQL.

    Returns a list of warning messages (empty = no drift detected).
    """
    warnings: list[str] = []

    # Build name -> dataset slug map from resolved payloads
    name_slug_map: dict[str, str] = {}
    for payload in support_payloads:
        slug = payload.get("dataset") or payload.get("name")
        if slug:
            name_slug_map[payload["name"]] = slug

    if not name_slug_map:
        return warnings

    # Find which support names are used via placeholder
    used_via_placeholder: set[str] = set()
    for match in re.finditer(
        r"\{support\.([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_.]*)\}", raw_sql
    ):
        used_via_placeholder.add(match.group(1))

    for name, slug in name_slug_map.items():
        if name in used_via_placeholder:
            continue

        escaped = re.escape(slug)
        for str_match in re.finditer(
            r"""['"](?:[^'"]*""" + escaped + r"""[^'"]*)['"]""",
            raw_sql,
        ):
            text = str_match.group(0)
            # Filtra falsi positivi: deve sembrare un path (slash, backslash, extension)
            if not any(c in text for c in ("/", "\\", ".")):
                continue
            warnings.append(
                f"Support '{slug}' referenced via hardcoded path "
                f"instead of a {{support.{name}.*}} placeholder: "
                f"{'' if not sql_label else sql_label + ' > '}{text[:120]}"
            )

    return warnings


def flatten_support_template_ctx(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    ctx: dict[str, Any] = {}
    for payload in payloads:
        name = payload["name"]
        ctx[f"support.{name}.outputs"] = payload["outputs"]
        ctx[f"support.{name}.mart"] = payload["mart"]
        if payload.get("clean"):
            ctx[f"support.{name}.clean"] = payload["clean"]
        for table, table_path in (payload.get("mart_by_table") or {}).items():
            ctx[f"support.{name}.mart.{table}"] = table_path
        if payload.get("path"):
            ctx[f"support.{name}.path"] = payload["path"]
    return ctx
