"""Catalogo unico dei tipi fonte RAW — single source of truth.

Ogni tipo fonte (``dataset.yml → raw.sources[].type``) è dichiarato UNA volta
qui: classe plugin, factory, fetch adapter, estensione default e documentazione
del contratto. Da questo catalogo sono generati:

- la registrazione plugin (``toolkit.core.registry``)
- il dispatch fetch (``fetch_payload``, sostituisce il vecchio
  ``_FETCH_DISPATCH`` + ``_fetch_fallback`` in ``raw/_fetch_utils``)
- la lista contratto AI (``contracts.pipeline._SOURCE_TYPES``)

Aggiungere un nuovo tipo fonte = aggiungere una voce qui (+ la classe plugin).
Niente più drift tra runtime e contratto.
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from toolkit.core.exceptions import DownloadError

_FETCH = Callable[..., tuple[bytes, str]]
_FACTORY = Callable[[type], Callable[..., Any]]


@dataclass(frozen=True)
class SourceTypeSpec:
    """Specifica di un tipo fonte RAW.

    Attributes:
        name: Chiave in ``raw.sources[].type`` (es. ``http_file``).
        description: Una riga per il contratto AI/CLI.
        args: Documentazione argomenti per il contratto (nome → descrizione).
        module: Modulo del plugin (None = built-in senza classe, es. script).
        class_name: Classe del plugin nel modulo.
        factory: Wrapper ``cls -> factory(**client)`` per la registry.
        fetch: Adapter ``(stype, client, formatted_args) -> (bytes, origin)``.
        ext_mode: Come derivare l'estensione output:
            ``fixed`` (usa ``fixed_ext``), ``from_url``, ``from_path``,
            ``from_origin`` (fallback ``.bin`` se origin assente).
        fixed_ext: Estensione fissa quando ``ext_mode="fixed"``.
        note: Nota opzionale per il contratto.
    """

    name: str
    description: str
    args: dict[str, str] = field(default_factory=dict)
    module: str | None = None
    class_name: str | None = None
    factory: _FACTORY | None = None
    fetch: _FETCH | None = None
    ext_mode: str = "fixed"
    fixed_ext: str = ".bin"
    note: str = ""


# ---------------------------------------------------------------------------
# Fetch adapters (spostati da raw/_fetch_utils.py — firma invariata)
# ---------------------------------------------------------------------------


def _sdmx_root_from_url(url: str) -> str:
    """Deriva il root SDMX da una URL scoperta dallo scout.

    Lo scout espone l'URL col path risorsa (es. ``.../sdmx/2.1/dataflow/IT1/22_289``
    o ``.../data/NAMA_10R_3GDP``): il plugin SdmxSource costruisce i path
    partendo dal root (``data/...``, ``dataflow/...``), quindi stripamo il
    segmento di risorsa.
    """
    base = url.split("?")[0].rstrip("/")
    for marker in ("/dataflow/", "/data/"):
        if marker in base:
            return base[: base.index(marker)]
    return base


def _fetch_ckan(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    from toolkit.core.registry import registry

    src = registry.create(stype, **(client or {}))
    sample_bytes = formatted_args.get("sample_bytes")
    if formatted_args.get("download_all", False):
        return src.fetch_all(
            formatted_args["portal_url"],
            str(formatted_args["dataset_id"]),
            sample_bytes=sample_bytes,
        )
    return src.fetch(
        formatted_args["portal_url"],
        str(formatted_args["resource_id"])
        if formatted_args.get("resource_id") is not None
        else None,
        str(formatted_args["dataset_id"]) if formatted_args.get("dataset_id") is not None else None,
        str(formatted_args["resource_name"])
        if formatted_args.get("resource_name") is not None
        else None,
        sample_bytes=sample_bytes,
    )


def _fetch_sdmx(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    from toolkit.core.registry import registry

    agency = str(formatted_args.get("agency") or "IT1")
    src_client = dict(client or {})
    # Endpoint esplicito per agenzie non-ISTAT/ESTAT/OECD: lo scout scopre l'URL
    # (spesso col path dataflow/data) → deriviamo il root SDMX e lo usiamo
    # come base per data e metadata. ISTAT e OECD resta sui default (esploradati +
    # sdmx.istat.it / sdmx.oecd.org), ESTAT è auto-risolto dal profilo.
    endpoint = formatted_args.get("endpoint")
    agency_upper = agency.upper()
    is_known_agency = agency == "IT1" or agency_upper == "ESTAT" or agency_upper.startswith("OECD")
    if endpoint and not is_known_agency:
        root = _sdmx_root_from_url(str(endpoint))
        src_client.setdefault("data_base_url", root)
        src_client.setdefault("metadata_base_url", root)
    src = registry.create(stype, **src_client)
    return src.fetch(
        agency,
        str(formatted_args["flow"]),
        # La versione è opzionale: il profilo Eurostat (ESTAT) la ignora
        # (numero mobile, mai nel path dati); ISTAT e OECD la richiedono (errore a valle).
        str(formatted_args.get("version") or ""),
        formatted_args.get("filters"),
    )


def _fetch_sparql(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    from toolkit.core.registry import registry

    src = registry.create(stype, **(client or {}))
    pages = int(formatted_args.get("pages", 1))
    step = int(formatted_args.get("step", 10000))
    pagination = formatted_args.get("pagination")
    return src.fetch(
        str(formatted_args["endpoint"]),
        str(formatted_args["query"]),
        str(formatted_args.get("accept_format", "csv")),
        pages=pages,
        step=step,
        pagination=pagination,
    )


def _fetch_dbnomics(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    from toolkit.core.registry import registry

    src = registry.create(stype, **(client or {}))
    dataset = str(formatted_args["dataset"])
    provider = str(formatted_args.get("provider") or "ISTAT")
    dimensions = formatted_args.get("dimensions") or {}
    observations = bool(formatted_args.get("observations", True))
    page_size = formatted_args.get("page_size")
    max_pages = formatted_args.get("max_pages")
    content = src.fetch(
        dataset,
        provider=provider,
        dimensions=dimensions,
        observations=observations,
        page_size=page_size,
        max_pages=max_pages,
    )
    return content, dataset


def _fetch_http_file(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    from toolkit.core.registry import registry

    src = registry.create(stype, **(client or {}))
    sample_bytes = formatted_args.get("sample_bytes")
    payload = src.fetch(formatted_args["url"], sample_bytes=sample_bytes)
    return payload, formatted_args["url"]


def _fetch_http_post_file(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    from toolkit.core.registry import registry

    src = registry.create(stype, **(client or {}))
    post_data = formatted_args.get("post_data")
    sample_bytes = formatted_args.get("sample_bytes")
    payload = src.fetch(formatted_args["url"], data=post_data, sample_bytes=sample_bytes)
    return payload, formatted_args["url"]


def _fetch_local_file(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    from toolkit.core.registry import registry

    src = registry.create(stype, **(client or {}))
    payload = src.fetch(formatted_args["path"])
    return payload, formatted_args["path"]


def _fetch_script(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    """Execute a script command and read its output as the raw data.

    The script is run in a subprocess inside the candidate root directory.
    ``{year}`` placeholders in ``command`` and ``output`` are resolved
    before execution.

    Expected config::

        raw:
          sources:
            - type: script
              args:
                command: "python preprocess.py raw_{year}.csv"
                output: "raw_{year}.csv"
    """
    # Security guard: script source disabled by default
    if os.environ.get("TOOLKIT_ALLOW_SCRIPT_SOURCE") != "1":
        raise DownloadError(
            "script source type is disabled by default. "
            "Set TOOLKIT_ALLOW_SCRIPT_SOURCE=1 to enable."
        )

    command: str = formatted_args.get("command", "")
    if not command:
        raise DownloadError("script source requires a 'command' in args")

    output_path = Path(formatted_args.get("output", "output.csv"))
    base_dir_str = formatted_args.get("_base_dir")
    candidate_root = Path(base_dir_str) if base_dir_str else Path.cwd()

    # Resolve output path and confine it under base_dir
    output_abs = (candidate_root / output_path).resolve()
    try:
        output_abs.relative_to(candidate_root.resolve())
    except ValueError:
        raise DownloadError(
            f"Output path {output_abs} is outside candidate base directory {candidate_root}"
        )

    result = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True,
        timeout=(client or {}).get("timeout", 600),
        cwd=candidate_root,
    )
    if result.returncode != 0:
        stderr_preview = result.stderr[:500] if result.stderr else "(no stderr)"
        raise DownloadError(f"Script failed (exit {result.returncode}): {stderr_preview}")

    if not output_abs.exists():
        raise DownloadError(f"Script did not produce expected output file: {output_abs}")

    payload = output_abs.read_bytes()
    return payload, str(output_path)


# ---------------------------------------------------------------------------
# Catalogo
# ---------------------------------------------------------------------------

SOURCE_TYPES: dict[str, SourceTypeSpec] = {
    spec.name: spec
    for spec in (
        SourceTypeSpec(
            name="http_file",
            description="Scarica un file da URL HTTP/HTTPS. Il tipo piu' comune.",
            args={
                "url": "URL del file (puo' contenere {year})",
                "filename": "nome locale (puo' contenere {year})",
            },
            module="toolkit.plugins.http_file",
            class_name="HttpFileSource",
            factory=lambda cls: lambda **client: cls(**client),
            fetch=_fetch_http_file,
            ext_mode="from_url",
        ),
        SourceTypeSpec(
            name="http_post_file",
            description="Come http_file ma con POST (per API che richiedono body).",
            args={
                "url": "URL dell'endpoint",
                "filename": "nome locale",
                "data": "body della POST (opzionale)",
            },
            module="toolkit.plugins.http_post_file",
            class_name="HttpPostFileSource",
            factory=lambda cls: lambda **client: cls(**client),
            fetch=_fetch_http_post_file,
            ext_mode="from_url",
        ),
        SourceTypeSpec(
            name="local_file",
            description="File gia' presente su disco. Usato per test e smoke.",
            args={
                "path": "path al file o directory",
                "filename": "pattern glob (opzionale)",
            },
            module="toolkit.plugins.local_file",
            class_name="LocalFileSource",
            factory=lambda cls: lambda **client: cls(),
            fetch=_fetch_local_file,
            ext_mode="from_path",
        ),
        SourceTypeSpec(
            name="ckan",
            description="Scarica risorse da un portale CKAN via API.",
            args={
                "portal_url": "URL del portale CKAN (es. https://dati.gov.it)",
                "package_id": "ID del dataset CKAN",
                "resource_id": "ID specifico della risorsa (opzionale)",
                "filename": "nome locale (opzionale)",
            },
            module="toolkit.plugins.ckan",
            class_name="CkanSource",
            factory=lambda cls: lambda **client: cls(**client),
            fetch=_fetch_ckan,
            ext_mode="from_url",
        ),
        SourceTypeSpec(
            name="sdmx",
            description=(
                "Scarica dati da API SDMX 2.1. Profilo ISTAT (agency IT1, default) "
                "e profilo Eurostat (agency ESTAT, TSV wide + JSON 2.0)."
            ),
            args={
                "agency": "Agenzia SDMX: IT1 (ISTAT, default) o ESTAT (Eurostat)",
                "flow": "ID del dataflow (es. NAMA_10R_3GDP, 22_289)",
                "version": (
                    "Versione dataflow. Obbligatoria per ISTAT; opzionale/ignorata "
                    "per ESTAT (numero mobile, mai nel path)"
                ),
                "filters": (
                    "Filtri per dimensione (es. {freq: A, geo: [IT, ITC4]}). "
                    "Valori validati contro i constraints dell'API"
                ),
                "endpoint": (
                    "URL SDMX scoperta dallo scout. Funzionale per agenzie "
                    "non-ISTAT/ESTAT (root derivato); ISTAT/ESTAT auto-risolvono"
                ),
            },
            module="toolkit.plugins.sdmx",
            class_name="SdmxSource",
            factory=lambda cls: lambda **client: cls(**client),
            fetch=_fetch_sdmx,
            ext_mode="fixed",
            fixed_ext=".csv",
            note=(
                "Output: CSV normalizzato. ISTAT: colonne con _label (JSON 2.1). "
                "ESTAT: [dims..., year, (month), value, flag] — le label si risolvono "
                "a valle con le codelist."
            ),
        ),
        SourceTypeSpec(
            name="sparql",
            description=(
                "Esegue query SPARQL SELECT su endpoint pubblico. POST-first con "
                "fallback GET; paginazione OFFSET o keyset robusta (retry su errore "
                "e pagina vuota da throttle WAF, stop su pagina corta)."
            ),
            args={
                "endpoint": "URL dello SPARQL endpoint",
                "query": "Query SPARQL SELECT",
                "filename": "nome locale (opzionale)",
                "accept_format": (
                    "'csv' (default) o 'sparql-results+json' (obbligatorio se "
                    "l'endpoint rifiuta GROUP BY in CSV, es. dati.camera.it)"
                ),
                "pages": (
                    "Numero massimo di pagine OFFSET (default 1 = nessuna paginazione). "
                    "Endpoint con limite righe/query (es. 10k) richiedono pages > 1."
                ),
                "step": "Righe per pagina (default 10000)",
                "pagination": (
                    "Config paginazione: {'mode': 'offset'} (default) oppure "
                    "{'mode': 'keyset', 'key': '?var'} — keyset inietta "
                    "FILTER(?var > last) invece di OFFSET: deterministico, compatibile "
                    "con Virtuoso che rifiuta ORDER BY+OFFSET (SR353). La query keyset "
                    "DEVE avere gia' ORDER BY ?var."
                ),
            },
            module="toolkit.plugins.sparql",
            class_name="SparqlSource",
            factory=lambda cls: lambda **client: cls(timeout=client.get("timeout", 60)),
            fetch=_fetch_sparql,
            ext_mode="fixed",
            fixed_ext=".csv",
        ),
        SourceTypeSpec(
            name="dbnomics",
            description=(
                "Scarica serie da DBnomics API v22 (mirror pubblico ISTAT/Eurostat/… "
                "senza rate-limit ISTAT). Output CSV lungo: series_code, provider, "
                "dataset, dims…, TIME_PERIOD, OBS_VALUE. Sostituto operativo per "
                "ISTAT quando esploradati è rate-limitato o ban."
            ),
            args={
                "dataset": "ID dataset DBnomics (es. 93_498) oppure PROVIDER/DATASET (es. ISTAT/93_498)",
                "provider": "Provider (default ISTAT)",
                "dimensions": (
                    "Filtri per dimensione SDMX: {REF_AREA: [IT], FREQ: [A], …}. "
                    "Valori stringa o lista. Serializzati come JSON per l'API."
                ),
                "observations": (
                    "Se true (default) scarica le osservazioni; false = solo metadati "
                    "serie (poco utile per raw)"
                ),
                "page_size": "Righe per pagina API (default 1000, max 1000)",
                "max_pages": "Pagine massime (default 200) — guardrail su cubi enormi",
                "filename": "nome locale (opzionale)",
            },
            module="toolkit.plugins.dbnomics",
            class_name="DbnomicsSource",
            factory=lambda cls: lambda **client: cls(**client),
            fetch=_fetch_dbnomics,
            ext_mode="fixed",
            fixed_ext=".csv",
            note=(
                "Nessun rate-limit ISTAT. Preferire filtri dimensions stretti; "
                "evitare dataset completi a grana comune senza LIMIT. "
                "Client: timeout/retries come http_file."
            ),
        ),
        SourceTypeSpec(
            name="script",
            description=(
                "Esegue un command di shell e usa il file di output come dato RAW. "
                "Disabilitato di default (gate ambientale)."
            ),
            args={
                "command": "command da eseguire (shell, puo' contenere {year})",
                "output": "path del file di output relativo al candidate (puo' contenere {year})",
            },
            fetch=_fetch_script,
            ext_mode="from_origin",
            note=(
                "Sicurezza: richiede TOOLKIT_ALLOW_SCRIPT_SOURCE=1. Il subprocess "
                "gira nel root del candidate; l'output e' confinato sotto base_dir."
            ),
        ),
    )
}


def source_type_names() -> list[str]:
    """Nomi di tutti i tipi fonte noti (ordinati)."""
    return sorted(SOURCE_TYPES)


def contract_source_types() -> list[dict[str, Any]]:
    """Genera la lista contratto AI/CLI dai tipi fonte (ordine del catalogo)."""
    out: list[dict[str, Any]] = []
    for spec in SOURCE_TYPES.values():
        entry: dict[str, Any] = {
            "type": spec.name,
            "description": spec.description,
            "args": dict(spec.args),
        }
        if spec.note:
            entry["note"] = spec.note
        out.append(entry)
    return out


def fetch_payload(
    stype: str,
    client: dict,
    formatted_args: dict,
    base_dir: Path | None = None,
) -> tuple[bytes, str]:
    """Dispatch fetch al fetch adapter del tipo fonte.

    Tipo sconosciuto → ValueError con elenco dei tipi noti (niente fallback
    che indovina la firma di ``fetch()``).
    """
    if base_dir is not None:
        formatted_args = {**formatted_args, "_base_dir": str(base_dir)}
    spec = SOURCE_TYPES.get(stype)
    if spec is None or spec.fetch is None:
        known = ", ".join(source_type_names())
        raise ValueError(f"Unknown raw source type: {stype!r}. Known types: {known}")
    return spec.fetch(stype, client, formatted_args)
