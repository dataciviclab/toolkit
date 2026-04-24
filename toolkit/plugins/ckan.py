from __future__ import annotations

import csv
import io
from urllib.parse import urlparse, urlunparse

import requests

from toolkit.core.exceptions import DownloadError


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

    def _get_json(self, url: str, params: dict[str, str]) -> dict:
        headers = {"User-Agent": self.user_agent}
        last_err: Exception | None = None
        for _ in range(max(1, self.retries)):
            try:
                response = requests.get(url, params=params, timeout=self.timeout, headers=headers)
                if response.status_code != 200:
                    raise DownloadError(f"HTTP {response.status_code} for {response.url}")
                data = response.json()
                if not data.get("success"):
                    raise DownloadError(f"CKAN API failed for {response.url}")
                return data
            except Exception as exc:
                last_err = exc
        raise DownloadError(str(last_err) if last_err else f"Failed to fetch CKAN metadata from {url}")

    def _download_bytes(self, url: str) -> bytes:
        headers = {"User-Agent": self.user_agent}
        last_err: Exception | None = None
        for _ in range(max(1, self.retries)):
            try:
                response = requests.get(url, timeout=self.timeout, headers=headers)
                if response.status_code != 200:
                    raise DownloadError(f"HTTP {response.status_code} for {url}")
                return response.content
            except Exception as exc:
                last_err = exc
        raise DownloadError(str(last_err) if last_err else f"Failed to fetch {url}")

    def _datastore_search(self, resource_id: str, api_base: str) -> bytes:
        url = _normalize_datastore_search_url(api_base)
        payload = self._get_json(url, {"id": resource_id})
        result = payload.get("result", {})
        records = result.get("records") or []
        if not records:
            raise DownloadError(
                f"CKAN datastore_search for resource {resource_id} returned no records"
            )
        # NOTE: other DownloadError from this method indicate empty-result only;
        # HTTP/API errors surface as requests exceptions, caught by outer try-except in fetch()
        fields = [f["id"] for f in result.get("fields") or []]
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=fields)
        writer.writeheader()
        # NOTE: CSV format does not distinguish None from empty string.
        # row.get(k) returns None for missing keys; csv.DictWriter emits '' for None.
        # If semantic distinction matters, a different format (JSONL) is needed.
        for row in records:
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
            return self._download_bytes(resolved_url), resolved_url
        except DownloadError:
            pass
        return None

    def fetch(
        self,
        portal_url: str,
        resource_id: str | None = None,
        dataset_id: str | None = None,
        resource_name: str | None = None,
        *,
        prefer_datastore: bool = True,
    ) -> tuple[bytes, str]:
        last_err: Exception | None = None

        if resource_id:
            api_url = _normalize_resource_show_url(portal_url)
            try:
                metadata = self._get_json(api_url, {"id": str(resource_id)})
                result = metadata.get("result") or {}
                outcome = self._try_resource(result, prefer_datastore, portal_url, api_url)
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
                    outcome = self._try_resource(resource, prefer_datastore, portal_url, api_url)
                    if outcome is not None:
                        return outcome
                    # Capture last error from the loop for a meaningful message
                    last_download_err = DownloadError(
                        f"Failed to fetch resource '{resource.get('name')}' ({resource.get('format')})"
                    )
                if last_download_err:
                    raise DownloadError(str(last_download_err))
                last_err = DownloadError(
                    f"All resource formats failed for package {package_identifier}"
                )
            except DownloadError:
                raise
            except Exception as exc:
                last_err = exc

        if last_err:
            raise DownloadError(str(last_err))
        raise DownloadError("CKAN source requires resource_id or dataset_id")
