from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from toolkit.core.artifacts import should_write
from toolkit.core.config import ensure_dict, parse_bool
from toolkit.core.metadata import (
    config_hash_for_year,
    merge_layer_manifest,
    sha256_bytes,
    write_metadata,
)
from toolkit.core.paths import RAW_VALIDATION, RAW_PROFILE, layer_year_dir, to_root_relative
from toolkit.core.registry import register_builtin_plugins
from toolkit.core.validation import write_validation_json
from toolkit.profile.raw import (
    sniff_source_file,
    profile_raw,
    write_raw_profile,
)
from toolkit.scaffold.clean import scaffold_clean_if_missing
from toolkit.raw._fetch_utils import (
    _choose_primary_output,
    _fetch_payload,
    _format_args,
    _generate_run_id,
    _infer_ext,
    _resolve_output_path,
)
from toolkit.raw.extractors import get_extractor
from toolkit.raw.validate import validate_raw_output


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

        formatted_args = _format_args(args, year)
        if sample_bytes is not None:
            formatted_args["sample_bytes"] = sample_bytes
        payload, origin = _fetch_payload(stype, client, formatted_args, base_dir=base_dir)
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

            logger.info(f"RAW -> {fpath}")

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
    profile_ctx = {"clean": clean_cfg or {}, "output": output_cfg or {}}
    if primary_output_path.exists() and primary_output_path.suffix.lower() in {
        ".csv",
        ".tsv",
        ".txt",
        ".xlsx",
        ".xls",
    }:
        try:
            profile_hints = sniff_source_file(primary_output_path)
            if should_write("profile", "raw_profile", profile_ctx):
                raw_profile = profile_raw(
                    out_dir,
                    dataset,
                    year,
                    read_cfg=(clean_cfg or {}).get("read", {}),
                    primary_file=primary_output_path,
                )
                profile_dir = out_dir / "_profile"
                write_raw_profile(profile_dir, raw_profile)
                logger.info("RAW profile -> %s", profile_dir / RAW_PROFILE)

                scaffold_clean_if_missing(
                    raw_profile.__dict__,
                    dataset,
                    year,
                    base_dir or Path("."),
                    clean_cfg,
                    logger,
                )
        except Exception as exc:
            logger.warning(
                "RAW profile/scaffold generation failed: %s: %s", type(exc).__name__, exc
            )

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

    # --- QA RAW ---
    result = validate_raw_output(out_dir, files_written)
    vpath = write_validation_json(out_dir / RAW_VALIDATION, result)
    merge_layer_manifest(
        out_dir,
        validation_path=vpath.name,
        outputs=[
            {"file": f["file"], "sha256": f["sha256"], "bytes": f["bytes"]} for f in files_written
        ],
        ok=result.ok,
        errors_count=len(result.errors),
        warnings_count=len(result.warnings),
        primary_output_file=primary_output_file,
        sources=[
            {"name": entry["name"], "output_file": entry["output_file"]}
            for entry in manifest_sources
        ],
    )

    if result.warnings:
        logger.warning(
            f"RAW QA warnings ({dataset} {year}): {len(result.warnings)} -> {vpath.name}"
        )
        for w in result.warnings[:10]:
            logger.warning(f" - {w}")

    if not result.ok:
        logger.error(f"RAW QA FAILED ({dataset} {year}) -> {vpath}")
        for e in result.errors[:20]:
            logger.error(f" - {e}")
        raise RuntimeError(f"RAW validation failed for {dataset} {year}. See {vpath}")
    else:
        logger.info(f"RAW QA OK ({dataset} {year}) -> {vpath.name}")

    output_bytes = sum(f.get("bytes", 0) for f in files_written) if files_written else None
    source_urls = list(
        dict.fromkeys(
            inp["origin"]
            for inp in inputs
            if inp.get("origin") and str(inp["origin"]).startswith("http")
        )
    )

    # Calcola righe/colonne del primary output (riusa csv_quick_shape da toolit.core)
    output_rows = None
    col_count = None
    if primary_output_path.exists() and primary_output_path.suffix.lower() in {
        ".csv",
        ".tsv",
        ".txt",
    }:
        try:
            from toolkit.core.duckdb_shape import csv_quick_shape

            shape = csv_quick_shape(str(primary_output_path))
            output_rows = shape.get("row_count_estimate")
            col_count = shape.get("column_count")
        except Exception:
            pass

    return {
        "output_bytes": output_bytes,
        "source_urls": source_urls,
        "output_rows": output_rows,
        "col_count": col_count,
    }
