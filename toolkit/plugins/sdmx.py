from __future__ import annotations

import csv
import io
import json
import xml.etree.ElementTree as ET
from typing import Iterable

import requests

from toolkit.core.exceptions import DownloadError

SDMX_NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
}


def _safe_text(value: str | None) -> str:
    return (value or "").strip()


def _normalize_base_url(url: str) -> str:
    return url.rstrip("/")


def _flow_ref(agency: str, flow: str, version: str) -> str:
    return f"{agency},{flow},{version}"


ISTAT_SDMX_BASE = "https://sdmx.istat.it/SDMXWS/rest"
ISTAT_ESPLORADATI_BASE = "https://esploradati.istat.it/SDMXWS/rest"


class SdmxSource:
    """Fetch SDMX data as a normalized CSV payload."""

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
        self.data_base_url = _normalize_base_url(
            data_base_url or ISTAT_ESPLORADATI_BASE
        )
        self.metadata_base_url = _normalize_base_url(
            metadata_base_url or ISTAT_SDMX_BASE
        )

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
        return self._candidate_base_urls(agency, self.metadata_base_url, ISTAT_ESPLORADATI_BASE)

    def _data_base_urls(self, agency: str) -> list[str]:
        return self._candidate_base_urls(agency, self.data_base_url, ISTAT_SDMX_BASE)

    def _is_retryable_fallback_error(self, exc: DownloadError) -> bool:
        text = str(exc).lower()
        return "endpoint timeout" in text or "endpoint error (http 5" in text

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
        headers = {"User-Agent": self.user_agent}
        if accept:
            headers["Accept"] = accept

        url = f"{_normalize_base_url(base_url)}/{path.lstrip('/')}"
        last_err: Exception | None = None
        for _ in range(max(1, self.retries)):
            try:
                response = requests.get(url, params=params, timeout=self.timeout, headers=headers)
                if response.status_code != 200:
                    if response.status_code == 404:
                        raise DownloadError(
                            f"SDMX query not found (HTTP 404) for {response.url}"
                        )
                    if 500 <= response.status_code <= 599:
                        raise DownloadError(
                            f"SDMX endpoint error (HTTP {response.status_code}) for {response.url}"
                        )
                    raise DownloadError(f"SDMX HTTP {response.status_code} for {response.url}")
                return response.text, response.url
            except requests.exceptions.Timeout as exc:
                last_err = DownloadError(f"SDMX endpoint timeout for {url}: {exc}")
            except requests.exceptions.ConnectionError as exc:
                last_err = DownloadError(f"SDMX endpoint timeout for {url}: {exc}")
            except Exception as exc:
                if isinstance(exc, DownloadError):
                    last_err = exc
                else:
                    last_err = DownloadError(str(exc))
        raise last_err or DownloadError(f"Failed to fetch {url}")

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
        xml_text, _origin = self._get_text_from_candidates(
            self._metadata_base_urls(agency),
            f"dataflow/{agency}/{flow}",
        )
        try:
            return ET.fromstring(xml_text)
        except ET.ParseError as exc:
            raise DownloadError(f"Invalid SDMX XML metadata for flow={flow}") from exc

    def _current_version(self, root: ET.Element) -> str:
        dataflow = root.find(".//str:Dataflow", SDMX_NS)
        if dataflow is None:
            raise DownloadError("SDMX dataflow not found")

        structure_ref = dataflow.find(".//str:Structure/Ref", SDMX_NS)
        if structure_ref is None:
            raise DownloadError("SDMX dataflow missing Structure/Ref")

        version = _safe_text(structure_ref.attrib.get("version"))
        return version

    def _preview_dimensions(self, agency: str, flow: str, version: str) -> list[str]:
        flow_ref = _flow_ref(agency, flow, version)
        payload, _origin = self._get_json(
            self._data_base_urls(agency),
            f"data/{flow_ref}/all",
            params={"firstNObservations": "0"},
        )
        structure = payload.get("structure") or {}
        dimensions = (structure.get("dimensions") or {}).get("series") or []
        result: list[str] = []
        for dim in dimensions:
            dim_id = str(dim.get("id") or "")
            if dim_id:
                result.append(dim_id)
        if not result:
            raise DownloadError(
                f"SDMX structure preview returned no series dimensions for {flow_ref}"
            )
        return result

    def _build_key(self, dimensions: list[str], filters: dict | None) -> str:
        filters = filters or {}
        unknown = sorted(set(filters.keys()) - set(dimensions))
        if unknown:
            raise DownloadError(
                "Unknown SDMX filter dimensions: " + ", ".join(unknown)
            )

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
        dimensions = self._preview_dimensions(agency, flow, version)
        key = self._build_key(dimensions, filters)
        payload, origin = self._get_json(self._data_base_urls(agency), f"data/{flow_ref}/{key}")
        header, rows = self._normalize_rows(payload)
        if not rows:
            raise DownloadError(f"SDMX data returned no rows for {agency}/{flow} and key={key}")
        return self._rows_to_csv(header, rows), origin
