"""Pre-flight check: validate config + probe reachability + preview CSV quality.

Implementazione condivisa tra CLI (``toolkit run preflight``) e MCP.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.cli.common import iter_selected_years, load_cfg_and_logger


def run_preflight(
    config: str | Path,
    *,
    years_arg: str | None = None,
) -> dict[str, Any]:
    """Pre-flight check per un dataset config.

    1. **Config check**: valida dataset.yml con ``validate_config``.
    2. **Probe + preview**: per ogni anno e ogni fonte remota:
       - ``http_file`` / ``http_post_file`` (CSV) → ``preview_url``
         (reachability + quality score + encoding + schema)
       - ``ckan`` → ``probe_url_headers`` (reachability)
       - ``local_file``, ``sdmx``, ``sparql`` → skip (non timeoutano)

    Returns:
        Report strutturato::

            {
                "config": str,
                "dataset": str,
                "source_id": str | None,
                "years": list[int],
                "config_check": {"ok": bool, "errors": [...], "warnings": [...]},
                "sources": [
                    {
                        "name": str,
                        "type": str,
                        "year": int,
                        "url": str | None,
                        "reachable": bool,
                        "status": str,
                        "resource_format": str | None,
                        "encoding": str | None,
                        "delim": str | None,
                        "columns": list[str] | None,
                        "quality_score": int | None,
                        "quality_verdict": str | None,
                    },
                    ...
                ],
                "status": "passed" | "failed",
            }
    """
    cfg, logger = load_cfg_and_logger(str(config))

    results: dict[str, Any] = {
        "config": str(config),
        "dataset": cfg.dataset,
        "source_id": cfg.source_id,
        "years": [],
        "config_check": {},
        "sources": [],
        "status": "passed",
    }

    # ── 1. Config check ────────────────────────────────────────────────
    from toolkit.core.dataset_loader import validate_config

    config_check = validate_config(str(config))
    results["config_check"] = config_check
    if not config_check.get("ok", False):
        results["status"] = "failed"

    # ── 2. Anni ─────────────────────────────────────────────────────────
    selected_years = iter_selected_years(cfg, year_arg=None, years_arg=years_arg)
    results["years"] = list(selected_years)

    # ── 3. Per anno, per fonte ──────────────────────────────────────────
    # Riutilizza _resolve_source di cmd_run per normalizzare le fonti
    # (dict vs oggetto) — stesso parsing di _run_probe.
    from toolkit.cli.cmd_run import _resolve_source as _norm

    # Cache URL → risultato probe. Se stesso URL per anni diversi,
    # riusa il risultato invece di rifare la chiamata HTTP.
    _probe_cache: dict[str, dict[str, Any]] = {}

    for year in selected_years:
        for src in cfg.raw.sources or []:
            resolved = _norm(src, year)
            stype = resolved["stype"]
            args = resolved["args"]
            name = resolved["name"]

            entry: dict[str, Any] = {
                "name": name,
                "type": stype,
                "year": year,
                "url": None,
                "reachable": True,
                "status": "skipped",
                "resource_format": None,
                "encoding": None,
                "delim": None,
                "columns": None,
                "quality_score": None,
                "quality_verdict": None,
            }

            if stype in ("http_file", "http_post_file"):
                url = resolved["url"]
                entry["url"] = url

                if not url:
                    entry["status"] = "no_url"
                    entry["reachable"] = False
                    results["status"] = "failed"
                    results["sources"].append(entry)
                    continue

                # Se stesso URL già processato per anno precedente, riusa
                if url in _probe_cache:
                    entry.update(_probe_cache[url])
                    entry["name"] = name
                    entry["type"] = stype
                    entry["year"] = year
                    entry["url"] = url
                    entry["status"] = "cached"
                    results["sources"].append(entry)
                    continue

                # preview_url fa probe + sniff + quality in un colpo solo
                from toolkit.profile.preview import preview_url

                preview = preview_url(url)
                entry["reachable"] = preview.reachable
                entry["http_status"] = preview.http_status
                entry["status"] = preview.status
                entry["resource_format"] = preview.resource_format
                entry["encoding"] = preview.encoding_suggested
                entry["delim"] = preview.delim_suggested
                entry["columns"] = preview.columns
                entry["quality_score"] = preview.quality_score
                entry["quality_verdict"] = preview.quality_verdict

                # Cache per anni successivi (se URL non ha {year})
                _probe_cache[url] = {
                    k: entry[k]
                    for k in (
                        "reachable",
                        "http_status",
                        "status",
                        "resource_format",
                        "encoding",
                        "delim",
                        "columns",
                        "quality_score",
                        "quality_verdict",
                    )
                    if k in entry
                }

                if not preview.reachable:
                    results["status"] = "failed"

            elif stype == "ckan":
                portal_url = args.get("portal_url", "")
                portal = portal_url.replace("{year}", str(year)) if portal_url else ""
                entry["url"] = portal

                if not portal:
                    entry["status"] = "no_url"
                    entry["reachable"] = False
                    results["status"] = "failed"
                    results["sources"].append(entry)
                    continue

                # Se stesso portal già processato per anno precedente, riusa
                if portal in _probe_cache:
                    entry.update(_probe_cache[portal])
                    entry["name"] = name
                    entry["type"] = stype
                    entry["year"] = year
                    entry["url"] = portal
                    entry["status"] = "cached"
                    results["sources"].append(entry)
                    continue

                from toolkit.scout.http import probe_url_headers

                try:
                    probe = probe_url_headers(portal)
                    entry["http_status"] = probe["status_code"]
                    entry["reachable"] = probe["status_code"] < 400
                    entry["status"] = "reachable" if entry["reachable"] else "unreachable"

                    _probe_cache[portal] = {
                        k: entry[k]
                        for k in (
                            "reachable",
                            "http_status",
                            "status",
                        )
                        if k in entry
                    }
                except Exception as exc:
                    entry["reachable"] = False
                    entry["status"] = f"error: {exc}"
                    results["status"] = "failed"

            # local_file, sdmx, sparql → skipped (entry defaults preserved)

            results["sources"].append(entry)

    return results
