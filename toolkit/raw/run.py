from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from toolkit.core.config import ensure_dict, parse_bool
from toolkit.core.exceptions import EmptyResultError
from toolkit.core.metadata import (
    config_hash_for_year,
    merge_layer_manifest,
    sha256_bytes,
    write_metadata,
)
from toolkit.core.paths import RAW_PROFILE, layer_year_dir, to_root_relative
from toolkit.core.logging import short_path
from toolkit.core.registry import register_builtin_plugins
from toolkit.profile.raw import (
    sniff_source_file,
    profile_raw,
    write_raw_profile,
)
from toolkit.raw._fetch_utils import (
    _choose_primary_output,
    _fetch_payload,
    _format_args,
    _generate_run_id,
    _infer_ext,
    _resolve_output_path,
)
from toolkit.raw.extractors import get_extractor
from toolkit.raw.incremental import (
    append_delta,
    build_since_clause,
)


def _baseline_filename(stype: str, name: str, formatted_args: dict, field_name: str) -> str:
    """Nome del file raw che fa da baseline per il fetch incrementale.

    Deve essere deterministico e **identico** al path di scrittura (che usa
    ``formatted_args``): ``filename`` esplicito (con ``{year}`` già sostituito),
    altrimenti ``{name}{ext}``. Con ``output_policy: overwrite`` è la stessa
    path che il run riscrive.

    Le sorgenti con estensione non deterministica (``from_origin``, es.
    ``script``) sono rifiutate: la baseline non sarebbe individuale.
    """
    explicit = formatted_args.get("filename")
    if explicit:
        return str(explicit)
    from toolkit.core.source_types import SOURCE_TYPES

    spec = SOURCE_TYPES.get(stype)
    if spec is not None and spec.ext_mode == "from_origin":
        raise ValueError(
            f"{field_name}.incremental: il source type '{stype}' ha estensione "
            "non deterministica (from_origin); serve args.filename esplicito"
        )
    return f"{name}{_infer_ext(stype, formatted_args)}"


def run_raw(
    dataset: str,
    year: int,
    root: str | None,
    raw_cfg: dict,
    logger,
    *,
    base_dir: Path | None = None,
    run_id: str | None = None,
    output_cfg: dict | None = None,
    clean_cfg: dict | None = None,
    sample_bytes: int | None = None,
    source_id: str | None = None,
):
    """
    Supporta:
    raw:
      extractor: {type, args}   # default extractor
      sources:
        - name: ...
          type: ...
          client: ...
          args: ...
          extractor: {type, args}  # override per source
    """

    # Normalize from _CompatModel to dict if needed
    raw_cfg = ensure_dict(raw_cfg)
    output_cfg = ensure_dict(output_cfg)
    clean_cfg = ensure_dict(clean_cfg)

    register_builtin_plugins()

    out_dir = layer_year_dir(root, "raw", dataset, year)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_id or _generate_run_id()

    default_extractor_spec = raw_cfg.get("extractor")
    default_extractor_fn, default_extractor_args = get_extractor(default_extractor_spec)
    output_policy = str(raw_cfg.get("output_policy", "versioned"))

    sources = raw_cfg.get("sources")
    if not sources:
        raise ValueError("raw.sources missing or empty in dataset.yml")

    files_written: list[dict] = []
    inputs: list[dict] = []
    manifest_sources: list[dict] = []

    for i, source in enumerate(sources):
        stype = source.get("type") or source.get("plugin") or "http_file"
        client = source.get("client", {}) or {}
        args = source.get("args", {}) or {}
        name = source.get("name") or source.get("id") or f"source_{i + 1}"
        source_written: list[str] = []

        # Skip source if year is specified and doesn't match the requested year
        source_year = source.get("year")
        if source_year is not None and int(source_year) != year:
            logger.info(f"SKIP source '{name}' (year={source_year} != requested year={year})")
            continue

        # ── Fetch incrementale (append-only) ────────────────────────────
        # La baseline è il raw del run precedente allo stesso path; il
        # watermark ne ricava il filtro {since}. Su checkout fresco (nessuna
        # baseline) il run resta full: corretto, solo più lento.
        incremental = source.get("incremental")
        baseline_path: Path | None = None
        since_clause = ""
        if incremental:
            if output_policy != "overwrite":
                raise ValueError(
                    f"raw.sources[{i}].incremental richiede raw.output_policy: overwrite "
                    f"(baseline stabile allo stesso path); trovato '{output_policy}'"
                )
            # La baseline va risolta sullo STESSO path di scrittura: gli args
            # vanno formattati ({year}) prima di cercarla, altrimenti un
            # filename tipo "voti_{year}.csv" non verrebbe mai trovato e il
            # run sovrascriverebbe la storia col delta.
            baseline_name = _baseline_filename(
                stype, name, _format_args(args, year), f"raw.sources[{i}]"
            )
            baseline_path = out_dir / baseline_name
            since_clause = build_since_clause(baseline_path, incremental)

        formatted_args = _format_args(args, year, extra={"since": since_clause})
        if sample_bytes is not None:
            formatted_args["sample_bytes"] = sample_bytes
        try:
            payload, origin = _fetch_payload(stype, client, formatted_args, base_dir=base_dir)
        except EmptyResultError:
            # "Niente di nuovo" è uno stato legittimo per un delta, non un
            # errore: con una baseline esistente il run è un no-op.
            if baseline_path is None or not since_clause:
                raise
            payload = b""
            origin = str(formatted_args.get("endpoint") or "")
            logger.info("RAW incrementale -> nessun dato nuovo (delta vuoto)")

        if baseline_path is not None and since_clause:
            payload = append_delta(baseline_path, payload)
            logger.info(
                "RAW incrementale -> %s (delta accodato al baseline)",
                short_path(baseline_path, root),
            )
        inputs.append(
            {
                "file": formatted_args.get("filename") or name,
                "bytes": len(payload),
                "sha256": sha256_bytes(payload),
                "origin": origin,
            }
        )

        # extractor per-source (se presente) altrimenti default
        extractor_spec = source.get("extractor")
        if extractor_spec:
            extractor_fn, extractor_args = get_extractor(extractor_spec)
        else:
            extractor_fn, extractor_args = default_extractor_fn, default_extractor_args

        extracted = extractor_fn(payload, extractor_args)  # dict filename->bytes

        # se identity: filename è "file.bin" -> rinomina con nome+estensione inferita
        if list(extracted.keys()) == ["file.bin"]:
            # se specifico filename, uso quello (stabile!)
            explicit = formatted_args.get("filename")
            if explicit:
                extracted = {explicit: payload}
            else:
                ext = _infer_ext(stype, formatted_args, origin=origin)
                extracted = {f"{name}{ext}": payload}

        # scrittura file
        for fname, content in extracted.items():
            fpath = _resolve_output_path(out_dir, fname, output_policy)
            fpath.write_bytes(content)
            rel_file = to_root_relative(fpath, out_dir)
            source_written.append(rel_file)

            files_written.append(
                {
                    "file": rel_file,
                    "bytes": len(content),
                    "sha256": sha256_bytes(content),
                    "source_name": name,
                    "source_type": stype,
                    "origin": origin,
                }
            )

            logger.info("RAW -> %s", short_path(fpath, root))

        manifest_sources.append(
            {
                "name": name,
                "output_file": source_written[0] if source_written else "",
                "primary": parse_bool(source.get("primary", False), f"raw.sources[{i}].primary"),
            }
        )

    primary_output_file = _choose_primary_output(manifest_sources, logger)
    primary_output_path = out_dir / primary_output_file
    profile_hints = None
    if primary_output_path.exists() and primary_output_path.suffix.lower() in {
        ".csv",
        ".tsv",
        ".txt",
        ".xlsx",
        ".xls",
        ".ods",
    }:
        try:
            profile_hints = sniff_source_file(primary_output_path)
            raw_profile = profile_raw(
                out_dir,
                dataset,
                year,
                read_cfg=(clean_cfg or {}).get("read", {}),
                primary_file=primary_output_path,
            )
            profile_dir = out_dir / "_profile"
            write_raw_profile(profile_dir, raw_profile)
            logger.debug("RAW profile -> %s", profile_dir / RAW_PROFILE)
        except Exception as exc:
            logger.warning("RAW profile generation failed: %s: %s", type(exc).__name__, exc)

    metadata_payload = {
        "layer": "raw",
        "dataset": dataset,
        "year": year,
        "run_id": run_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "config_hash": config_hash_for_year(base_dir, year),
        "inputs": inputs,
        "outputs": [
            {"file": f["file"], "sha256": f["sha256"], "bytes": f["bytes"]} for f in files_written
        ],
        "files": files_written,
        "profile_hints": profile_hints,
    }
    if source_id:
        metadata_payload["source_id"] = source_id
    write_metadata(out_dir, metadata_payload)

    # Track output files in metadata (validation is done by run_raw_validation)
    merge_layer_manifest(
        out_dir,
        outputs=[
            {"file": f["file"], "sha256": f["sha256"], "bytes": f["bytes"]} for f in files_written
        ],
        primary_output_file=primary_output_file,
        sources=[
            {"name": entry["name"], "output_file": entry["output_file"]}
            for entry in manifest_sources
        ],
    )

    output_bytes = sum(f.get("bytes", 0) for f in files_written) if files_written else None
    source_urls = list(
        dict.fromkeys(
            inp["origin"]
            for inp in inputs
            if inp.get("origin") and str(inp["origin"]).startswith("http")
        )
    )

    # Legge righe/colonne dal raw_profile appena scritto (se disponibile)
    output_rows = None
    col_count = None
    try:
        profile_dir = out_dir / "_profile"
        profile_path = profile_dir / RAW_PROFILE
        if profile_path.exists():
            from toolkit.core.io import read_json_or_none as _read_json

            prof = _read_json(profile_path) or {}
            output_rows = prof.get("row_count")
            col_count = len(prof.get("columns_raw", [])) if prof.get("columns_raw") else None
    except Exception:
        pass

    return {
        "output_bytes": output_bytes,
        "source_urls": source_urls,
        "output_rows": output_rows,
        "col_count": col_count,
    }
