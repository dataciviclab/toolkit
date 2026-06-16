from __future__ import annotations

import csv
import io
import zipfile
from typing import Any
from urllib.parse import urlparse, urlunparse

from lab_connectors.http import HttpClient

from toolkit.core.exceptions import DownloadError
from toolkit.plugins._http_utils import truncate_at_line


def _normalize_datastore_search_url(portal_url: str) -> str:
    base = portal_url.rstrip("/")
    if base.endswith("/api/3/action"):
        return f"{base}/datastore_search"
    if base.endswith("/api/3"):
        return f"{base}/action/datastore_search"
    return f"{base}/api/3/action/datastore_search"


def _normalize_resource_show_url(portal_url: str) -> str:
    base = portal_url.rstrip("/")
    if base.endswith("/api/3/action"):
        return f"{base}/resource_show"
    if base.endswith("/api/3"):
        return f"{base}/action/resource_show"
    return f"{base}/api/3/action/resource_show"


def _normalize_package_show_url(portal_url: str) -> str:
    base = portal_url.rstrip("/")
    if base.endswith("/api/3/action"):
        return f"{base}/package_show"
    if base.endswith("/api/3"):
        return f"{base}/action/package_show"
    return f"{base}/api/3/action/package_show"


def _force_https(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme == "http":
        return urlunparse(parsed._replace(scheme="https"))
    return url


class CkanSource:
    """Resolve a CKAN resource via resource_show, then download its current URL."""

    def __init__(self, timeout: int = 60, retries: int = 2, user_agent: str | None = None):
        self.timeout = timeout
        self.retries = retries
        self.user_agent = user_agent or "dataciviclab-toolkit/0.1"
        self._client = HttpClient(
            timeout=timeout,
            max_retries=retries,
            user_agent=self.user_agent,
        )

    def _get_json(self, url: str, params: dict[str, Any]) -> dict:
        result = self._client.get(url, params=params)
        if result.is_ok and result.response is not None:
            response = result.response
            if response.status_code != 200:
                raise DownloadError(f"HTTP {response.status_code} for {response.url}")
            data = response.json()
            if not data.get("success"):
                raise DownloadError(f"CKAN API failed for {response.url}")
            return data
        err = result.err
        raise DownloadError(str(err) if err else f"Failed to fetch CKAN metadata from {url}")

    def _download_bytes(self, url: str, sample_bytes: int | None = None) -> bytes:
        headers = None
        if sample_bytes is not None:
            headers = {"Range": f"bytes=0-{sample_bytes - 1}"}
        result = self._client.get(url, headers=headers)
        if result.is_ok and result.response is not None:
            response = result.response
            if response.status_code not in (200, 206):
                raise DownloadError(f"HTTP {response.status_code} for {url}")
            content = response.content
            if sample_bytes is not None:
                content = truncate_at_line(content, sample_bytes)
            return content
        err = result.err
        raise DownloadError(str(err) if err else f"Failed to fetch {url}")

    def _datastore_search(self, resource_id: str, api_base: str, page_size: int = 32000) -> bytes:
        """Fetch ALL rows from a CKAN DataStore resource, paginating if needed.

        CKAN DataStore default limit is 100 rows; API max is 32000.
        Questa funzione pagina automaticamente fino a esaurimento records.
        """
        url = _normalize_datastore_search_url(api_base)
        all_records: list[dict] = []
        fields: list[str] = []
        total: int | None = None
        offset = 0
        limit = min(page_size, 32000)

        while True:
            params: dict[str, str | int] = {
                "id": resource_id,
                "limit": limit,
                "offset": offset,
            }
            payload = self._get_json(url, params)
            result = payload.get("result", {})
            records = result.get("records") or []
            if not records and total is None:
                raise DownloadError(
                    f"CKAN datastore_search for resource {resource_id} returned no records"
                )

            # Capture field list from first response
            if not fields:
                fields = [f["id"] for f in result.get("fields") or []]
                if not fields and records:
                    fields = list(records[0].keys())

            all_records.extend(records)

            # Stop when we have all records
            if total is None:
                total = result.get("total") or len(records)
            if len(all_records) >= total:
                break

            # No-progress guard: server may cap at fewer records than limit
            if not records:
                break

            # Advance by actual records returned, not by requested limit
            # (alcuni portali CKAN ignorano limit=32000 e restituiscono
            #  il loro default, es. 100. Offset su limit salterebbe dati
            #  o causerebbe loop infinito.)
            offset += len(records)

        if not all_records:
            raise DownloadError(
                f"CKAN datastore_search for resource {resource_id} returned no records"
            )

        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=fields)
        writer.writeheader()
        # NOTE: CSV format does not distinguish None from empty string.
        # row.get(k) returns None for missing keys; csv.DictWriter emits '' for None.
        # If semantic distinction matters, a different format (JSONL) is needed.
        for row in all_records:
            writer.writerow({k: row.get(k) for k in fields})
        return buffer.getvalue().encode("utf-8")

    def _select_resource_from_package(
        self,
        result: dict,
        resource_id: str | None,
        resource_name: str | None = None,
    ) -> list[dict]:
        resources = result.get("resources") or []
        if not resources:
            raise DownloadError("CKAN package_show returned no resources")

        with_url = [item for item in resources if item.get("url")]
        if not with_url:
            raise DownloadError("CKAN package_show returned resources without URL")

        if resource_id:
            for item in with_url:
                if str(item.get("id")) == str(resource_id):
                    return [item]
            raise DownloadError(
                f"CKAN package_show did not contain requested resource_id={resource_id}"
            )

        if resource_name:
            wanted = str(resource_name).strip().lower()
            for item in with_url:
                candidate = str(item.get("name") or "").strip().lower()
                if candidate == wanted:
                    return [item]
            for item in with_url:
                candidate = str(item.get("name") or "").strip().lower()
                if wanted in candidate:
                    return [item]
            raise DownloadError(
                f"CKAN package_show did not contain requested resource_name={resource_name}"
            )

        def _score(item: dict) -> tuple[int, str]:
            fmt = str(item.get("format") or "").lower()
            url = str(item.get("url") or "").lower()
            if "csv" in fmt or ".csv" in url:
                rank = 0
            elif "zip" in fmt or ".zip" in url:
                rank = 1
            elif "xlsx" in fmt or ".xlsx" in url or "xls" in fmt or ".xls" in url:
                rank = 2
            elif "json" in fmt or ".json" in url:
                rank = 3
            elif "xml" in fmt or ".xml" in url:
                rank = 4
            else:
                rank = 9
            return rank, str(item.get("name") or "")

        return sorted(with_url, key=_score)

    def _resource_is_datastore_active(self, resource: dict) -> bool:
        return str(resource.get("datastore_active") or "").lower() == "true"

    def _try_resource(
        self,
        resource: dict,
        prefer_datastore: bool,
        portal_url: str,
        api_base: str,
        sample_bytes: int | None = None,
    ) -> tuple[bytes, str] | None:
        """Try to fetch a single resource. Returns (bytes, url) or None if all attempts fail."""
        resource_id = str(resource.get("id") or "")
        if prefer_datastore and self._resource_is_datastore_active(resource):
            try:
                return self._datastore_search(resource_id, portal_url), api_base
            except DownloadError:
                pass
        resolved_url = _force_https(str(resource["url"]))
        try:
            return self._download_bytes(resolved_url, sample_bytes=sample_bytes), resolved_url
        except DownloadError:
            pass
        return None

    @staticmethod
    def _is_csv_resource(resource: dict) -> bool:
        """True if the resource is tabular CSV data (including CSV inside ZIP).

        Uses the CKAN ``format`` field rather than URL extension, because
        portals like ANAC tag JSON/TTL bundles as "JSON"/"TTL" even when
        the download URL ends with ``.zip``.

        Excludes metadata/log resources (e.g. ``*logCsv*`` on ANAC) that
        happen to have format=CSV but are not actual data tables.
        """
        fmt = str(resource.get("format") or "").lower()
        if fmt not in ("csv", "zip", "text/csv"):
            return False
        name = str(resource.get("name") or "").lower()
        if "log" in name:
            return False
        return True

    @staticmethod
    def _extract_csv_from_zip(data: bytes) -> bytes:
        """Extract first CSV from a ZIP archive. Returns the CSV content as bytes."""
        zf = zipfile.ZipFile(io.BytesIO(data))
        csv_members = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_members:
            raise DownloadError("ZIP archive contains no CSV file")
        return zf.read(csv_members[0])

    def fetch_all(
        self,
        portal_url: str,
        dataset_id: str,
        *,
        prefer_datastore: bool = True,
        sample_bytes: int | None = None,
    ) -> tuple[bytes, str]:
        """Fetch ALL CSV/ZIP resources from a CKAN dataset and concatenate them into one CSV.

        Useful for datasets with monthly files (e.g. ANAC CIG) where you need
        all resources, not just the best-ranked one.

        Returns (concatenated_csv_bytes, dataset_id).
        """
        api_url = _normalize_package_show_url(portal_url)
        metadata = self._get_json(api_url, {"id": dataset_id})
        result = metadata.get("result") or {}
        resources = result.get("resources") or []
        if not resources:
            raise DownloadError(f"CKAN package_show returned no resources for dataset {dataset_id}")

        csv_resources = [r for r in resources if self._is_csv_resource(r)]
        if not csv_resources:
            raise DownloadError(
                f"No CSV/ZIP resources found in dataset {dataset_id} "
                f"(available formats: {set(r.get('format', '?').upper() for r in resources)})"
            )

        all_chunks: list[bytes] = []
        downloaded = 0
        for i, resource in enumerate(csv_resources):
            res_name = resource.get("name", f"resource_{i}")
            try:
                raw, url = self._try_resource(
                    resource, prefer_datastore, portal_url, api_url, sample_bytes=sample_bytes
                )
            except DownloadError:
                continue
            if raw is None:
                continue

            # Extract CSV if the downloaded content is a ZIP archive
            # (many CKAN portals serve CSV files inside ZIP even when
            #  the format field says "CSV", e.g. ANAC).
            if raw[:2] == b"PK":
                try:
                    raw = self._extract_csv_from_zip(raw)
                except DownloadError:
                    # valid ZIP without CSV inside → skip this resource
                    continue

            # Skip header on subsequent files
            if i == 0:
                all_chunks.append(raw)
            else:
                first_nl = raw.find(b"\n")
                if first_nl != -1:
                    all_chunks.append(raw[first_nl + 1 :])
                else:
                    all_chunks.append(raw)
            downloaded += 1

        if not all_chunks:
            raise DownloadError(f"Failed to download any resource from dataset {dataset_id}")

        # Concatenate all CSV chunks. Each chunk already ends with its own
        # newline (``\\n`` or ``\\r\\n``), so we use ``b""`` join — not
        # ``b"\\n"`` — to avoid doubling line terminators.
        # Return a synthetic URL ending with .csv so the toolkit infers the
        # right extension.
        synthetic_url = f"{dataset_id}_all.csv"
        return b"".join(all_chunks), synthetic_url

    def fetch(
        self,
        portal_url: str,
        resource_id: str | None = None,
        dataset_id: str | None = None,
        resource_name: str | None = None,
        *,
        prefer_datastore: bool = True,
        sample_bytes: int | None = None,
    ) -> tuple[bytes, str]:
        last_err: Exception | None = None

        if resource_id:
            api_url = _normalize_resource_show_url(portal_url)
            try:
                metadata = self._get_json(api_url, {"id": str(resource_id)})
                result = metadata.get("result") or {}
                outcome = self._try_resource(
                    result, prefer_datastore, portal_url, api_url, sample_bytes=sample_bytes
                )
                if outcome is not None:
                    return outcome
                # Fallback: try datastore even if prefer_datastore=False, when URL is absent.
                if self._resource_is_datastore_active(result):
                    try:
                        return self._datastore_search(str(resource_id), portal_url), api_url
                    except DownloadError:
                        pass
                last_err = DownloadError(
                    f"CKAN resource_show returned no URL for resource_id={resource_id}"
                )
            except Exception as exc:
                last_err = exc

        package_identifier = dataset_id or resource_id
        if package_identifier:
            api_url = _normalize_package_show_url(portal_url)
            try:
                metadata = self._get_json(api_url, {"id": str(package_identifier)})
                result = metadata.get("result") or {}
                ranked_resources = self._select_resource_from_package(
                    result, resource_id, resource_name
                )
                last_download_err: Exception | None = None
                for resource in ranked_resources:
                    outcome = self._try_resource(
                        resource, prefer_datastore, portal_url, api_url, sample_bytes=sample_bytes
                    )
                    if outcome is not None:
                        return outcome
                    # Capture last error from the loop for a meaningful message
                    last_download_err = DownloadError(
                        f"Failed to fetch resource '{resource.get('name')}' ({resource.get('format')})"
                    )
                if last_download_err:
                    raise DownloadError(str(last_download_err))
            except DownloadError:
                raise
            except Exception as exc:
                last_err = exc

        if last_err:
            raise DownloadError(str(last_err))
        raise DownloadError("CKAN source requires resource_id or dataset_id")
