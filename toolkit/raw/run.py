from __future__ import annotations

from pathlib import Path
import hashlib
from datetime import datetime
from urllib.parse import urlparse

from toolkit.core.metadata import write_metadata
from toolkit.core.paths import layer_year_dir
from toolkit.plugins import register_plugins
from toolkit.core.registry import registry
from toolkit.raw.extractors import get_extractor
from toolkit.raw.validate import validate_raw_output, write_raw_validation


def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _format_args(args: dict, year: int) -> dict:
    formatted = {}
    for k, v in (args or {}).items():
        formatted[k] = v.format(year=year) if isinstance(v, str) else v
    return formatted

def _infer_ext(stype: str, formatted_args: dict) -> str:
    if stype == "http_file":
        url = formatted_args.get("url", "")
        # 1) prova suffix del PATH senza query
        parsed = urlparse(url)
        suffix = Path(parsed.path).suffix
        if suffix:
            return suffix

        # 2) euristica: se dentro l'url c'è ".csv" o "csv" → .csv
        low = url.lower()
        if ".csv" in low or "csv" in low:
            return ".csv"
        if ".zip" in low or "zip" in low:
            return ".zip"

        return ".bin"

    if stype == "local_file":
        return Path(formatted_args["path"]).suffix or ".bin"

    return ".bin"

def _fetch_payload(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    src = registry.create(stype, **(client or {}))
    if stype == "http_file":
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


def run_raw(dataset: str, year: int, root: str | None, raw_cfg: dict, logger):
    """
    Supporta:
    - legacy:
      raw:
        source: {type, client, args}
    - nuovo:
      raw:
        extractor: {type, args}   # default extractor
        sources:
          - name: ...
            type: ...
            client: ...
            args: ...
            extractor: {type, args}  # override per source
    """

    register_plugins()

    out_dir = layer_year_dir(root, "raw", dataset, year)
    out_dir.mkdir(parents=True, exist_ok=True)

    default_extractor_spec = raw_cfg.get("extractor")
    default_extractor_fn, default_extractor_args = get_extractor(default_extractor_spec)

    # -------- build sources list (retrocompat) --------
    sources = raw_cfg.get("sources")
    if not sources:
        # fallback legacy
        legacy = raw_cfg.get("source", {})
        sources = [legacy]

    files_written: list[dict] = []

    for i, source in enumerate(sources):
        stype = source.get("type") or source.get("plugin") or "http_file"
        client = source.get("client", {}) or {}
        args = source.get("args", {}) or {}
        name = source.get("name") or source.get("id") or f"source_{i+1}"

        formatted_args = _format_args(args, year)
        payload, origin = _fetch_payload(stype, client, formatted_args)

        # extractor per-source (se presente) altrimenti default
        extractor_spec = source.get("extractor")
        if extractor_spec:
            extractor_fn, extractor_args = get_extractor(extractor_spec)
        else:
            extractor_fn, extractor_args = default_extractor_fn, default_extractor_args

        extracted = extractor_fn(payload, extractor_args)  # dict filename->bytes

        # se identity: filename è "file.bin" → rinomina con nome+estensione inferita
        if list(extracted.keys()) == ["file.bin"]:
            # se specifico filename, uso quello (stabile!)
            explicit = formatted_args.get("filename")
            if explicit:
                extracted = {explicit: payload}
            else:
                ext = _infer_ext(stype, formatted_args)
                extracted = {f"{name}{ext}": payload}

        # scrittura file
        for fname, content in extracted.items():
            fpath = out_dir / fname
            fpath.write_bytes(content)

            files_written.append({
                "file": fname,
                "bytes": len(content),
                "sha256": _sha256(content),
                "source_name": name,
                "source_type": stype,
                "origin": origin,
            })

            logger.info(f"RAW → {fpath}")

    write_metadata(out_dir, {
        "layer": "raw",
        "dataset": dataset,
        "year": year,
        "timestamp_utc": datetime.utcnow().isoformat(),
        "files": files_written,
    })

    # --- QA RAW ---
    result = validate_raw_output(out_dir, files_written)
    vpath = write_raw_validation(out_dir, result)

    if result.warnings:
        logger.warning(f"RAW QA warnings ({dataset} {year}): {len(result.warnings)} → {vpath.name}")
        for w in result.warnings[:10]:
            logger.warning(f" - {w}")

    if not result.ok:
        logger.error(f"RAW QA FAILED ({dataset} {year}) → {vpath}")
        for e in result.errors[:20]:
            logger.error(f" - {e}")
        raise RuntimeError(f"RAW validation failed for {dataset} {year}. See {vpath}")
    else:
        logger.info(f"RAW QA OK ({dataset} {year}) → {vpath.name}")