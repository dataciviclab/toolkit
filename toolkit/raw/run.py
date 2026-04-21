from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from toolkit.core.artifacts import resolve_artifact_policy, should_write
from toolkit.core.manifest import write_raw_manifest
from toolkit.core.config import parse_bool
from toolkit.core.metadata import config_hash_for_year, sha256_bytes, write_metadata
from toolkit.core.paths import layer_year_dir, to_root_relative
from toolkit.core.registry import register_builtin_plugins, registry
from toolkit.core.validation import write_validation_json
from toolkit.profile.raw import build_profile_hints, profile_raw, write_raw_profile, write_suggested_read_yml
from toolkit.raw.extractors import get_extractor
from toolkit.raw.validate import validate_raw_output


def _format_args(args: dict, year: int) -> dict:
    formatted = {}
    for k, v in (args or {}).items():
        formatted[k] = v.format(year=year) if isinstance(v, str) else v
    return formatted


def _infer_ext(stype: str, formatted_args: dict, origin: str | None = None) -> str:
    if stype == "sdmx":
        return ".csv"
    if stype in {"http_file", "ckan"}:
        url = origin or formatted_args.get("url", "")
        parsed = urlparse(url)
        path = parsed.path or ""
        low_path = path.lower()

        # Some providers expose files behind php endpoints.
        # Prefer the meaningful extension and never keep ".php".
        if low_path.endswith(".csv.php"):
            return ".csv"
        if low_path.endswith(".zip.php"):
            return ".zip"

        suffix = Path(path).suffix.lower()
        if suffix and suffix != ".php":
            return suffix

        # fallback heuristics on full URL/query
        low = url.lower()
        if ".csv" in low or "csv" in low:
            return ".csv"
        if ".zip" in low or "zip" in low:
            return ".zip"

        return ".bin"

    if stype == "local_file":
        p = Path(formatted_args["path"])
        low_name = p.name.lower()
        if low_name.endswith(".csv.php"):
            return ".csv"
        if low_name.endswith(".zip.php"):
            return ".zip"

        suffix = p.suffix.lower()
        if suffix and suffix != ".php":
            return suffix
        return ".bin"

    return ".bin"


def _fetch_payload(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    src = registry.create(stype, **(client or {}))
    if stype == "ckan":
        payload, origin = src.fetch(
            formatted_args["portal_url"],
            str(formatted_args["resource_id"]) if formatted_args.get("resource_id") is not None else None,
            str(formatted_args["dataset_id"]) if formatted_args.get("dataset_id") is not None else None,
            str(formatted_args["resource_name"]) if formatted_args.get("resource_name") is not None else None,
        )
    elif stype == "sdmx":
        payload, origin = src.fetch(
            str(formatted_args.get("agency") or "IT1"),
            str(formatted_args["flow"]),
            str(formatted_args["version"]),
            formatted_args.get("filters"),
        )
    elif stype == "http_file":
        payload = src.fetch(formatted_args["url"])
        origin = formatted_args["url"]
    elif stype == "local_file":
        payload = src.fetch(formatted_args["path"])
        origin = formatted_args["path"]
    else:
        first_val = next(iter(formatted_args.values()))
        payload = src.fetch(first_val)
        origin = str(first_val)
    return payload, origin


def _next_available_path(out_dir: Path, fname: str) -> Path:
    candidate = out_dir / fname
    if not candidate.exists():
        return candidate

    stem = Path(fname).stem
    suffix = Path(fname).suffix
    i = 1
    while True:
        candidate = out_dir / f"{stem}_{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def _generate_run_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"


def _resolve_output_path(out_dir: Path, fname: str, policy: str) -> Path:
    candidate = out_dir / fname
    if policy == "overwrite":
        return candidate
    if policy == "versioned":
        return _next_available_path(out_dir, fname)
    raise ValueError("raw.output_policy must be one of: overwrite, versioned")


def _choose_primary_output(source_outputs: list[dict], logger) -> str:
    available = [entry for entry in source_outputs if entry.get("output_file")]
    if not available:
        raise RuntimeError("RAW manifest cannot determine primary output file because no outputs were written.")

    primary_marked = [entry for entry in available if entry.get("primary")]
    if primary_marked:
        if len(primary_marked) > 1:
            logger.warning(
                "RAW manifest found multiple sources with primary: true; using the first one."
            )
        return str(primary_marked[0]["output_file"])

    if len(available) == 1:
        return str(available[0]["output_file"])

    logger.warning(
        "RAW manifest primary_output_file defaulting to the first source. "
        "Set raw.sources[].primary: true to choose explicitly."
    )
    return str(available[0]["output_file"])


def run_raw(
    dataset: str,
    year: int,
    root: str | None,
    raw_cfg: dict,
    logger,
    *,
    base_dir: Path | None = None,
    run_id: str | None = None,
    strict_plugins: bool = False,
    output_cfg: dict | None = None,
    clean_cfg: dict | None = None,
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

    register_builtin_plugins(strict=strict_plugins)

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

        formatted_args = _format_args(args, year)
        payload, origin = _fetch_payload(stype, client, formatted_args)
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
    policy = resolve_artifact_policy(output_cfg)
    if primary_output_path.exists() and primary_output_path.suffix.lower() in {".csv", ".tsv", ".txt"}:
        try:
            profile_hints = build_profile_hints(primary_output_path)
            if should_write("profile", "suggested_read", policy, profile_ctx):
                conservative_hints = dict(profile_hints)
                conservative_hints["decimal_suggested"] = None
                suggested_path = write_suggested_read_yml(out_dir / "_profile", conservative_hints)
                logger.info("RAW suggested_read -> %s", suggested_path)

            if should_write("profile", "raw_profile", policy, profile_ctx):
                from toolkit.scaffold.clean import generate_clean_sql
                raw_profile = profile_raw(out_dir, dataset, year)
                profile_dir = out_dir / "_profile"
                write_raw_profile(
                    profile_dir,
                    raw_profile,
                    write_canonical=True,
                    write_legacy_alias=should_write("profile", "profile_alias", policy, profile_ctx),
                )
                logger.info("RAW profile -> %s", profile_dir / "raw_profile.json")

                clean_sql_path = Path(base_dir) / (clean_cfg or {}).get("sql", "sql/clean.sql")
                if not clean_sql_path.exists():
                    scaffold_sql = generate_clean_sql(raw_profile.__dict__, dataset, year)
                    clean_sql_path.parent.mkdir(parents=True, exist_ok=True)
                    clean_sql_path.write_text(scaffold_sql, encoding="utf-8")
                    logger.info("scaffold clean.sql -> %s", clean_sql_path)
                else:
                    logger.info("clean.sql gia esistente, scaffold saltato (%s)", clean_sql_path)
        except Exception as exc:
            logger.warning("RAW profile/scaffold generation failed: %s: %s", type(exc).__name__, exc)

    metadata_path = write_metadata(
        out_dir,
        {
            "layer": "raw",
            "dataset": dataset,
            "year": year,
            "run_id": run_id,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "config_hash": config_hash_for_year(base_dir, year),
            "inputs": inputs,
            "outputs": [
                {"file": f["file"], "sha256": f["sha256"], "bytes": f["bytes"]}
                for f in files_written
            ],
            "files": files_written,
            "profile_hints": profile_hints,
        },
    )

    # --- QA RAW ---
    result = validate_raw_output(out_dir, files_written)
    vpath = write_validation_json(out_dir / "raw_validation.json", result)
    write_raw_manifest(
        out_dir,
        {
            "dataset": dataset,
            "year": year,
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "sources": [
                {"name": entry["name"], "output_file": entry["output_file"]}
                for entry in manifest_sources
            ],
            "primary_output_file": primary_output_file,
            "metadata": metadata_path.name,
            "validation": vpath.name,
            "summary": {
                "ok": result.ok,
                "errors_count": len(result.errors),
                "warnings_count": len(result.warnings),
            },
            "outputs": [
                {"file": f["file"], "sha256": f["sha256"], "bytes": f["bytes"]}
                for f in files_written
            ],
        },
    )

    if result.warnings:
        logger.warning(f"RAW QA warnings ({dataset} {year}): {len(result.warnings)} -> {vpath.name}")
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
    source_urls = list(dict.fromkeys(
        inp["origin"] for inp in inputs if inp.get("origin") and str(inp["origin"]).startswith("http")
    ))
    return {"output_bytes": output_bytes, "source_urls": source_urls}
