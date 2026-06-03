from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

from lab_connectors.http import HttpClient

from toolkit.core.exceptions import DownloadError

logger = logging.getLogger("toolkit.plugins.http_file")

# Estensioni che NON possono essere troncate: formati binari, compressi,
# o contentitori i cui metadati sono in coda al file.
# Campionare questi formati con HTTP Range produce file corrotti.
_NON_TRUNCABLE_EXTS: set[str] = {
    ".parquet",
    ".zip",
    ".xlsx",
    ".xls",
    ".gz",
    ".bz2",
    ".7z",
    ".rar",
}


class HttpFileSource:
    """Download a file via HTTP(S) with SSL fallback for expired/invalid certs.

    Adapter over lab_connectors.http.HttpClient that translates
    HttpResult into toolkit's DownloadError contract.
    """

    def __init__(self, timeout: int = 60, retries: int = 2, user_agent: str | None = None):
        self.timeout = timeout
        self.retries = retries
        self.user_agent = user_agent or "dataciviclab-toolkit/0.1"
        self._client = HttpClient(
            timeout=timeout,
            max_retries=retries,
            user_agent=self.user_agent,
        )

    @staticmethod
    def _is_non_truncable(url: str) -> bool:
        """Restituisce True se l'estensione del file non è troncabile in modo sicuro."""
        path = urlparse(url).path
        suffix = Path(path).suffix.lower()
        # URL senza estensione o con parametri di query: assumiamo troncabile
        if not suffix:
            return False
        return suffix in _NON_TRUNCABLE_EXTS

    def fetch(self, url: str, sample_bytes: int | None = None) -> bytes:
        if sample_bytes is not None and self._is_non_truncable(url):
            logger.info(
                "sample_bytes=%s ignorato per formato non troncabile: %s",
                sample_bytes,
                url,
            )
            sample_bytes = None

        headers = None
        if sample_bytes is not None:
            headers = {"Range": f"bytes=0-{sample_bytes - 1}"}
        result = self._client.get(url, headers=headers)
        if result.is_ok and result.response is not None:
            if result.response.status_code not in (200, 206):
                raise DownloadError(f"HTTP {result.response.status_code} for {url}")
            content = result.response.content
            # Troncamento locale: server che ignorano Range (200 invece di 206)
            # restituiscono tutto il file. Taglia per garantire il limite byte,
            # poi tronca all'ultima linea completa (evita CSV con quote non
            # chiuse, JSON troncato, ecc.).
            if sample_bytes is not None and len(content) > sample_bytes:
                content = content[:sample_bytes]
                # Trova l'ultimo newline per chiudere l'ultima linea completa
                last_newline = content.rfind(b"\n")
                if last_newline > 0:
                    content = content[: last_newline + 1]
            return content
        err = result.err
        raise DownloadError(str(err) if err else f"Failed to fetch {url}")
