from __future__ import annotations

import csv
import io
import json
import re
import xml.etree.ElementTree as ET
from typing import Iterable

import requests

from lab_connectors.http import HttpClient

from toolkit.core.exceptions import DownloadError

SDMX_NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
}

# ── Eurostat (ESTAT) — profilo agenzia dedicato ───────────────────────────────
# L'API Eurostat non segue la convenzione SDMX-REST canonica di ISTAT:
#   - dati: `data/{flow}/{key}?format=TSV` (nessuna versione nel path;
#     la versione è un numero mobile che cambia a ogni release);
#   - formato nativo: TSV wide (anni come colonne, flag in coda ai valori) —
#     Eurostat risponde 406 su `text/csv` e serve SDMX-JSON **2.0** (flat
#     `dimension.{id}.category.{index,label}`), non il 2.1 di ISTAT;
#   - le label delle dimensioni NON sono nel TSV: vanno risolte a valle
#     (clean.sql via codelists), a differenza del profilo ISTAT che le porta
#     nel payload JSON.
EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"
EUROSTAT_AGENCY = "ESTAT"
EUROSTAT_TIME_DIM = "time"  # dimensione TIME nel JSON 2.0 di Eurostat

# Valore mancante e flag di qualità nel TSV Eurostat (SDMX-TSV): il flag è una
# lettera finale dopo spazio/virgole (es. `37300 d`, `: ` per missing).
TSV_MISSING = ":"
TSV_FLAG_RE = re.compile(r"\s+([a-z])$")
TSV_MONTHLY_RE = re.compile(r"^\d{4}-\d{2}$")


def _safe_text(value: str | None) -> str:
    return (value or "").strip()


def _normalize_base_url(url: str) -> str:
    return url.rstrip("/")


def _flow_ref(agency: str, flow: str, version: str) -> str:
    return f"{agency},{flow},{version}"


ISTAT_SDMX_BASE = "https://sdmx.istat.it/SDMXWS/rest"
ISTAT_ESPLORADATI_BASE = "https://esploradati.istat.it/SDMXWS/rest"


def _parse_tsv_value(raw: str) -> tuple[float | None, str | None]:
    """Parse a Eurostat TSV value cell: number + optional quality flag."""
    s = raw.strip()
    if not s or s == TSV_MISSING:
        return None, None
    m = TSV_FLAG_RE.search(s)
    flag = m.group(1) if m else None
    num = s[: m.start()].strip() if m else s
    try:
        return float(num), flag
    except ValueError:
        return None, flag


def _detect_tsv_dims(raw_header: str) -> list[str]:
    """Detect dimension names from the first column of the TSV header.

    Header format: ``dim1,dim2,...,dimN\\TIME_PERIOD\\t2020\\t2021...``
    """
    first_col = raw_header.strip().split("\t")[0]
    backslash_pos = first_col.find("\\")
    dims_raw = first_col[:backslash_pos] if backslash_pos > 0 else first_col
    return [d.strip() for d in dims_raw.split(",") if d.strip()]


def _tsv_to_csv(text: str) -> tuple[list[str], list[dict[str, object]]]:
    """Unpivot a Eurostat SDMX-TSV payload into long CSV rows.

    Produces ``[dim1..dimN, year, (month), value, flag]`` — lo stesso contratto
    di ``connectors/tsv_normalize.py`` del repo eurostat. Il TSV è wide: ogni
    riga è una serie, le colonne successive alla prima sono i periodi.
    """
    lines = text.splitlines()
    if not lines:
        raise DownloadError("Empty Eurostat TSV payload")

    raw_header = lines[0]
    dims = _detect_tsv_dims(raw_header)

    parts = raw_header.strip().split("\t")
    year_cols = [y.strip() for y in parts[1:] if y.strip()]
    is_monthly = bool(year_cols and TSV_MONTHLY_RE.match(year_cols[0]))

    fieldnames = list(dims) + (
        ["year", "month", "value", "flag"] if is_monthly else ["year", "value", "flag"]
    )

    rows: list[dict[str, object]] = []
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue
        cols = line.split("\t")
        if not cols:
            continue

        dim_values = [v.strip() for v in cols[0].split(",")]
        base_row: dict[str, object] = {}
        for i, d in enumerate(dims):
            base_row[d] = dim_values[i] if i < len(dim_values) else ""

        for i, period in enumerate(year_cols):
            if i + 1 >= len(cols):
                continue
            value, flag = _parse_tsv_value(cols[i + 1])
            row = dict(base_row)
            if is_monthly:
                ym = period.split("-")
                row["year"] = ym[0]
                row["month"] = str(int(ym[1])) if len(ym) > 1 else "1"
            else:
                row["year"] = period
            row["value"] = "" if value is None else value
            row["flag"] = flag or ""
            rows.append(row)

    if not rows:
        raise DownloadError("Eurostat TSV returned no rows")

    return fieldnames, rows


class SdmxSource:
    """Fetch SDMX data as a normalized CSV payload."""

    # Cache di struttura per flow (per istanza): evita richieste HTTP duplicate
    # quando la pipeline esegue più fetch consecutivi sullo stesso SdmxSource.
    # NON condivisa tra istanze per evitare contaminazione tra endpoint diversi.
    # Formato: _dataflow_cache[agency/flow] = ET.Element
    #         _constraints_cache[agency/flow/version] = dict[str, list[str]]

    def __init__(
        self,
        timeout: int = 60,
        retries: int = 2,
        user_agent: str | None = None,
        data_base_url: str | None = None,
        metadata_base_url: str | None = None,
    ):
        self.timeout = timeout
        self.retries = retries
        self.user_agent = user_agent or "dataciviclab-toolkit/0.1"
        self.data_base_url = _normalize_base_url(data_base_url or ISTAT_ESPLORADATI_BASE)
        self.metadata_base_url = _normalize_base_url(metadata_base_url or ISTAT_SDMX_BASE)
        # Distinguiamo "default ISTAT" da "URL passato dall'utente": il profilo
        # ESTAT usa automaticamente l'endpoint Eurostat se non esplicitato.
        self._data_base_url_given = data_base_url is not None
        self._metadata_base_url_given = metadata_base_url is not None
        self._client = HttpClient(
            timeout=timeout,
            max_retries=retries,
            user_agent=self.user_agent,
        )
        self._dataflow_cache: dict[str, ET.Element] = {}
        self._constraints_cache: dict[str, dict[str, list[str]]] = {}

    def _candidate_base_urls(self, agency: str, primary: str, alternate: str) -> list[str]:
        normalized_primary = _normalize_base_url(primary)
        if agency != "IT1":
            return [normalized_primary]

        urls = [normalized_primary]
        normalized_alternate = _normalize_base_url(alternate)
        if normalized_alternate not in urls:
            urls.append(normalized_alternate)
        return urls

    def _metadata_base_urls(self, agency: str) -> list[str]:
        if self._is_estat(agency) and not self._metadata_base_url_given:
            return [_normalize_base_url(EUROSTAT_BASE)]
        return self._candidate_base_urls(agency, self.metadata_base_url, ISTAT_ESPLORADATI_BASE)

    def _data_base_urls(self, agency: str) -> list[str]:
        if self._is_estat(agency) and not self._data_base_url_given:
            return [_normalize_base_url(EUROSTAT_BASE)]
        return self._candidate_base_urls(agency, self.data_base_url, ISTAT_SDMX_BASE)

    def _is_retryable_fallback_error(self, exc: DownloadError) -> bool:
        text = str(exc).lower()
        return "endpoint timeout" in text or "endpoint error (http 5" in text

    # ── Profilo Eurostat (ESTAT) ───────────────────────────────────────────

    @staticmethod
    def _is_estat(agency: str) -> bool:
        """True se la fonte è l'agenzia Eurostat (convenzioni API dedicate).

        Eurostat serve SDMX-JSON 2.0 (non 2.1), rifiuta `text/csv` (406) e non
        accetta la versione nel path dati. Il profilo ESTAT dirama fetch e
        constraints su logica dedicata; ogni altra agenzia mantiene il
        comportamento esistente (convenzione ISTAT).
        """
        return _safe_text(agency).upper() == EUROSTAT_AGENCY

    def _estat_constraints(self, flow: str) -> dict[str, list[str]]:
        """Valid codes per dimension da SDMX-JSON 2.0 di Eurostat.

        La struttura è piatta: ``dimension.{id}.category.{index,label}`` con
        l'ordine serializzato della mappa = ordine SDMX delle dimensioni.
        TIME viene esclusa (nel TSV è l'header delle colonne, non una serie).
        """
        cache_key = f"{EUROSTAT_AGENCY}/{flow}"
        if cache_key in self._constraints_cache:
            return self._constraints_cache[cache_key]
        payload, _origin = self._get_json(
            self._data_base_urls(EUROSTAT_AGENCY),
            f"data/{flow}/all",
            params={"format": "JSON", "lastNObservations": "0"},
        )
        dimension = payload.get("dimension") or {}
        result: dict[str, list[str]] = {}
        for dim_id in dimension:
            if dim_id == EUROSTAT_TIME_DIM:
                continue
            category = (dimension.get(dim_id) or {}).get("category") or {}
            result[dim_id] = [str(c) for c in (category.get("index") or {})]
        if not result:
            raise DownloadError(f"SDMX-Eurostat returned no dimensions for flow={flow}")
        self._constraints_cache[cache_key] = result
        return result

    def _estat_fetch_tsv(self, flow: str, key: str) -> tuple[bytes, str]:
        """Fetch dati Eurostat in formato TSV (wide) e normalizza in CSV long."""
        path = f"data/{flow}/{key}" if key and key != "all" else f"data/{flow}/all"
        tsv_text, origin = self._get_text_from_candidates(
            self._data_base_urls(EUROSTAT_AGENCY),
            path,
            accept="text/tab-separated-values",
            params={"format": "TSV"},
        )
        header, rows = _tsv_to_csv(tsv_text)
        return self._rows_to_csv(header, rows), origin

    def fetch_codelist(self, codelist_id: str, agency: str = EUROSTAT_AGENCY) -> dict[str, object]:
        """Fetch una codelist SDMX (SDMX-JSON 2.0) e la normalizza.

        Supportato per il profilo Eurostat (ESTAT): restituisce
        ``{id, codes: {code: label}, annotations: {code: {type: value}},
        origin}``. Le annotazioni includono ``LEVEL`` (gerarchia NUTS per GEO:
        0=country, 1/2/3=NUTS1/2/3) e ``IS_STANDARD_CODE``.

        Il repo eurostat la usa per rigenerare ``codelists/*.csv`` al posto di
        ``scripts/update_codelists.py``; il formato canonico delle colonne
        resta scelta del chiamante.
        """
        if not self._is_estat(agency):
            raise DownloadError("fetch_codelist è supportato solo per il profilo Eurostat (ESTAT)")
        payload, origin = self._get_json(
            self._data_base_urls(agency),
            f"codelist/{agency}/{codelist_id}",
            params={"format": "json"},
        )
        category = payload.get("category") or {}
        labels = category.get("label") or {}
        annotations_raw = (payload.get("extension") or {}).get("code-annotation") or {}
        annotations: dict[str, dict[str, str]] = {}
        for code, ann_list in annotations_raw.items():
            merged: dict[str, str] = {}
            for ann in ann_list or []:
                ann_type = str(ann.get("type") or "")
                if ann_type:
                    merged[ann_type] = str(ann.get("title") or ann.get("text") or "")
            if merged:
                annotations[str(code)] = merged
        return {
            "id": codelist_id,
            "codes": {str(k): str(v) for k, v in labels.items()},
            "annotations": annotations,
            "origin": origin,
        }

    def _get_text_from_candidates(
        self,
        base_urls: Iterable[str],
        path: str,
        *,
        accept: str | None = None,
        params: dict[str, str] | None = None,
    ) -> tuple[str, str]:
        base_url_list = list(base_urls)
        last_err: DownloadError | None = None
        for idx, base_url in enumerate(base_url_list):
            try:
                return self._get_text(base_url, path, accept=accept, params=params)
            except DownloadError as exc:
                last_err = exc
                has_more = idx < len(base_url_list) - 1
                if not has_more or not self._is_retryable_fallback_error(exc):
                    raise
        raise last_err or DownloadError("Failed to fetch SDMX resource")

    def _get_text(
        self,
        base_url: str,
        path: str,
        *,
        accept: str | None = None,
        params: dict[str, str] | None = None,
    ) -> tuple[str, str]:
        url = f"{_normalize_base_url(base_url)}/{path.lstrip('/')}"
        custom_headers: dict[str, str] = {}
        if accept:
            custom_headers["Accept"] = accept

        result = self._client.get(url, params=params, headers=custom_headers)

        if result.is_ok and result.response is not None:
            response = result.response
            if response.status_code != 200:
                if response.status_code == 404:
                    raise DownloadError(f"SDMX query not found (HTTP 404) for {response.url}")
                if 500 <= response.status_code <= 599:
                    raise DownloadError(
                        f"SDMX endpoint error (HTTP {response.status_code}) for {response.url}"
                    )
                raise DownloadError(f"SDMX HTTP {response.status_code} for {response.url}")
            return response.text, response.url

        err = result.err
        if err is None:
            raise DownloadError(f"Failed to fetch {url}")
        # Preserve SDMX-specific diagnostics for timeout/connection errors
        if isinstance(err, requests.exceptions.Timeout):
            raise DownloadError(f"SDMX endpoint timeout for {url}: {err}")
        if isinstance(err, requests.exceptions.ConnectionError):
            raise DownloadError(f"SDMX endpoint connection error for {url}: {err}")
        raise DownloadError(str(err))

    def _get_json(
        self,
        base_urls: Iterable[str],
        path: str,
        *,
        params: dict[str, str] | None = None,
    ) -> tuple[dict, str]:
        text, origin = self._get_text_from_candidates(
            base_urls,
            path,
            accept="application/json",
            params=params,
        )
        try:
            return json.loads(text), origin
        except json.JSONDecodeError as exc:
            raise DownloadError(f"Invalid SDMX JSON payload from {origin}") from exc

    def _get_dataflow(self, agency: str, flow: str) -> ET.Element:
        cache_key = f"{agency}/{flow}"
        if cache_key in self._dataflow_cache:
            return self._dataflow_cache[cache_key]
        xml_text, _origin = self._get_text_from_candidates(
            self._metadata_base_urls(agency),
            f"dataflow/{agency}/{flow}",
        )
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            raise DownloadError(f"Invalid SDMX XML metadata for flow={flow}") from exc
        self._dataflow_cache[cache_key] = root
        return root

    def _current_version(self, root: ET.Element) -> str:
        dataflow = root.find(".//str:Dataflow", SDMX_NS)
        if dataflow is None:
            raise DownloadError("SDMX dataflow not found")

        structure_ref = dataflow.find(".//str:Structure/Ref", SDMX_NS)
        if structure_ref is None:
            raise DownloadError("SDMX dataflow missing Structure/Ref")

        version = _safe_text(structure_ref.attrib.get("version"))
        return version

    def preview_constraints(self, agency: str, flow: str, version: str) -> dict[str, list[str]]:
        """Return valid codes per dimension for a dataflow.

        Useful to validate filters before calling fetch(), or to understand
        which values are available without downloading data.

        Falls back to empty constraints (all wildcard) when the SDMX endpoint
        does not support JSON for this dataflow.
        """
        if self._is_estat(agency):
            return self._estat_constraints(flow)
        cache_key = f"{agency}/{flow}/{version}"
        if cache_key in self._constraints_cache:
            return self._constraints_cache[cache_key]
        flow_ref = _flow_ref(agency, flow, version)
        try:
            payload, _origin = self._get_json(
                self._data_base_urls(agency),
                f"data/{flow_ref}/all",
                params={"firstNObservations": "0"},
            )
        except DownloadError:
            # JSON not available for this dataflow (e.g. ISTAT XML-only).
            # Return empty constraints: fetch() will use wildcard key and
            # fall back to CSV for the actual data.
            self._constraints_cache[cache_key] = {}
            return {}
        structure = payload.get("structure") or {}
        dimensions = structure.get("dimensions") or {}
        result: dict[str, list[str]] = {}
        for section in ("series", "observation"):
            for dim in dimensions.get(section) or []:
                dim_id = str(dim.get("id") or "")
                if not dim_id or dim_id in result:
                    continue
                values: list[dict] = dim.get("values") or []
                result[dim_id] = [str(v.get("id") or "") for v in values if v.get("id")]
        self._constraints_cache[cache_key] = result
        return result

    def _build_key(self, dimensions: list[str], filters: dict | None) -> str:
        filters = filters or {}
        unknown = sorted(set(filters.keys()) - set(dimensions))
        if unknown:
            raise DownloadError("Unknown SDMX filter dimensions: " + ", ".join(unknown))

        if not dimensions:
            return "all"

        parts: list[str] = []
        for dim in dimensions:
            value = filters.get(dim)
            if value is None:
                parts.append("")
                continue
            if isinstance(value, (list, tuple)):
                token = "+".join(str(item) for item in value)
            else:
                token = str(value)
            parts.append(token)

        key = ".".join(parts).rstrip(".")
        return key or "all"

    def _dimension_value(self, dimension: dict, index_token: str) -> tuple[str, str]:
        values = dimension.get("values") or []
        idx = int(index_token)
        if idx >= len(values):
            raise DownloadError(
                f"SDMX dimension index {idx} out of range for {dimension.get('id')}"
            )
        entry = values[idx]
        code = str(entry.get("id") or "")
        label = str(entry.get("name") or code)
        return code, label

    def _normalize_rows(self, payload: dict) -> tuple[list[str], list[dict[str, object]]]:
        structure = payload.get("structure") or {}
        dimensions = structure.get("dimensions") or {}
        series_dims = dimensions.get("series") or []
        observation_dims = dimensions.get("observation") or []

        header: list[str] = []
        for dim in series_dims:
            dim_id = str(dim.get("id") or "")
            if dim_id:
                header.extend([dim_id, f"{dim_id}_label"])
        for dim in observation_dims:
            dim_id = str(dim.get("id") or "")
            if dim_id:
                header.extend([dim_id, f"{dim_id}_label"])
        header.append("value")

        rows: list[dict[str, object]] = []
        for dataset in payload.get("dataSets") or []:
            for series_key, series_val in (dataset.get("series") or {}).items():
                series_parts = series_key.split(":") if series_key else []
                series_ctx: dict[str, object] = {}
                for idx, token in enumerate(series_parts):
                    if idx >= len(series_dims):
                        continue
                    dim = series_dims[idx]
                    dim_id = str(dim.get("id") or "")
                    code, label = self._dimension_value(dim, token)
                    series_ctx[dim_id] = code
                    series_ctx[f"{dim_id}_label"] = label

                for obs_key, obs_val in (series_val.get("observations") or {}).items():
                    row = dict(series_ctx)
                    obs_parts = obs_key.split(":") if obs_key else []
                    for idx, token in enumerate(obs_parts):
                        if idx >= len(observation_dims):
                            continue
                        dim = observation_dims[idx]
                        dim_id = str(dim.get("id") or "")
                        code, label = self._dimension_value(dim, token)
                        row[dim_id] = code
                        row[f"{dim_id}_label"] = label

                    if isinstance(obs_val, list) and obs_val:
                        row["value"] = obs_val[0]
                    else:
                        row["value"] = obs_val
                    rows.append(row)

        return header, rows

    def _rows_to_csv(self, header: list[str], rows: list[dict[str, object]]) -> bytes:
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col) for col in header})
        return buffer.getvalue().encode("utf-8")

    def fetch(
        self,
        agency: str,
        flow: str,
        version: str,
        filters: dict | None = None,
    ) -> tuple[bytes, str]:
        agency = _safe_text(agency) or "IT1"
        flow = _safe_text(flow)
        version = _safe_text(version)

        if not flow:
            raise DownloadError("SDMX source requires flow")

        # Profilo Eurostat: versione non richiesta (numero mobile, mai nel path)
        if self._is_estat(agency):
            return self._fetch_estat(flow, version, filters)

        if not version:
            raise DownloadError("SDMX source requires version")

        dataflow_root = self._get_dataflow(agency, flow)
        current_version = self._current_version(dataflow_root)

        if current_version != version:
            raise DownloadError(
                f"Requested SDMX version {version} for {agency}/{flow} is not available; "
                f"current version is {current_version}"
            )

        flow_ref = _flow_ref(agency, flow, version)
        constraints = self.preview_constraints(agency, flow, version)
        key = self._build_key(list(constraints.keys()), filters)
        for dim, allowed in constraints.items():
            if not allowed:
                continue
            val = filters.get(dim) if filters else None
            if val is None:
                continue
            if isinstance(val, (list, tuple)):
                invalid = [v for v in val if str(v) not in allowed]
            else:
                invalid = [val] if str(val) not in allowed else []
            if invalid:
                ellipsis = " ..." if len(allowed) > 10 else ""
                raise DownloadError(
                    f"Invalid value(s) for SDMX dimension {dim}: {invalid} — "
                    f"allowed: {allowed[:10]}{ellipsis}"
                )
        # Try JSON first (provides _label columns via structure metadata).
        # Fall back to CSV for SDMX endpoints that return non-JSON (e.g. XML).
        # Transport errors (connection, 404, 5xx) are NOT silently fallback —
        # only content-type mismatches where the server returns 200 but not JSON.
        fetch_text, origin = self._get_text_from_candidates(
            self._data_base_urls(agency),
            f"data/{flow_ref}/{key}",
            accept="application/json",
        )
        try:
            payload = json.loads(fetch_text)
        except json.JSONDecodeError:
            # Response is not JSON (e.g. ISTAT XML). Try CSV instead.
            csv_text, origin = self._get_text_from_candidates(
                self._data_base_urls(agency),
                f"data/{flow_ref}/{key}",
                accept="text/csv",
            )
            return csv_text.encode("utf-8"), origin

        header, rows = self._normalize_rows(payload)
        if not rows:
            raise DownloadError(f"SDMX data returned no rows for {agency}/{flow} and key={key}")
        return self._rows_to_csv(header, rows), origin

    def _fetch_estat(
        self,
        flow: str,
        version: str,
        filters: dict | None = None,
    ) -> tuple[bytes, str]:
        """Fetch Eurostat data via TSV (SDMX-JSON 2.0 non è parsabile dal path
        ISTAT e `text/csv` risponde 406). La versione è ignorata: Eurostat la
        serve come numero mobile fuori dal path dati.

        Output: CSV ``[dim1..dimN, year, (month), value, flag]`` — stesso
        contratto di ``connectors/tsv_normalize.py`` del repo eurostat (le
        label di dimensione vanno risolte a valle con le codelist).
        """
        constraints = self._estat_constraints(flow)
        key = self._build_key(list(constraints.keys()), filters)
        for dim, allowed in constraints.items():
            if not allowed:
                continue
            val = filters.get(dim) if filters else None
            if val is None:
                continue
            if isinstance(val, (list, tuple)):
                invalid = [v for v in val if str(v) not in allowed]
            else:
                invalid = [val] if str(val) not in allowed else []
            if invalid:
                ellipsis = " ..." if len(allowed) > 10 else ""
                raise DownloadError(
                    f"Invalid value(s) for SDMX dimension {dim}: {invalid} — "
                    f"allowed: {allowed[:10]}{ellipsis}"
                )
        return self._estat_fetch_tsv(flow, key)
