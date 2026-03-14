from __future__ import annotations

from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin

import requests
import typer


_CANDIDATE_EXTENSIONS = (".csv", ".xlsx", ".xls", ".zip", ".json")
_DEFAULT_USER_AGENT = "dataciviclab-toolkit/scout-url"
_DEFAULT_TIMEOUT = 10
_MAX_PRINTED_LINKS = 20


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.hrefs.append(value)
                return


def _is_html(content_type: str | None) -> bool:
    if not content_type:
        return False
    return "html" in content_type.lower()


def _is_file_like(final_url: str, content_type: str | None, content_disposition: str | None) -> bool:
    lowered_url = final_url.lower()
    if any(ext in lowered_url for ext in _CANDIDATE_EXTENSIONS):
        return True
    if content_disposition and "attachment" in content_disposition.lower():
        return True
    if content_type and not _is_html(content_type):
        lowered_type = content_type.lower()
        return any(
            token in lowered_type
            for token in (
                "csv",
                "excel",
                "spreadsheetml",
                "zip",
                "json",
            )
        )
    return False


def _candidate_links(base_url: str, html_text: str) -> list[str]:
    parser = _AnchorParser()
    parser.feed(html_text)
    links: list[str] = []
    seen: set[str] = set()
    for href in parser.hrefs:
        lowered = href.lower()
        if not any(ext in lowered for ext in _CANDIDATE_EXTENSIONS):
            continue
        absolute = urljoin(base_url, href)
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
    return links


def probe_url(url: str, *, timeout: int = _DEFAULT_TIMEOUT) -> dict[str, Any]:
    headers = {"User-Agent": _DEFAULT_USER_AGENT}
    response = requests.get(url, allow_redirects=True, timeout=timeout, headers=headers)
    content_type = response.headers.get("Content-Type")
    content_disposition = response.headers.get("Content-Disposition")
    final_url = response.url
    is_html = _is_html(content_type)

    if is_html:
        response.encoding = response.encoding or response.apparent_encoding or "utf-8"
        candidate_links = _candidate_links(final_url, response.text)
        kind = "html"
    elif _is_file_like(final_url, content_type, content_disposition):
        candidate_links = []
        kind = "file"
    else:
        candidate_links = []
        kind = "opaque"

    return {
        "requested_url": url,
        "final_url": final_url,
        "status_code": response.status_code,
        "content_type": content_type,
        "content_disposition": content_disposition,
        "kind": kind,
        "candidate_links": candidate_links,
    }


def scout_url(
    url: str = typer.Argument(..., help="URL da ispezionare"),
    timeout: int = typer.Option(_DEFAULT_TIMEOUT, "--timeout", min=1, help="Timeout HTTP in secondi"),
) -> None:
    """
    Ispeziona un URL per dataset scouting minimale.
    """
    try:
        result = probe_url(url, timeout=timeout)
    except requests.RequestException as exc:
        typer.echo(f"error: {type(exc).__name__}: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo(f"requested_url: {result['requested_url']}")
    typer.echo(f"final_url: {result['final_url']}")
    typer.echo(f"status_code: {result['status_code']}")
    typer.echo(f"content_type: {result['content_type']}")
    typer.echo(f"content_disposition: {result['content_disposition']}")
    typer.echo(f"kind: {result['kind']}")

    if result["candidate_links"]:
        typer.echo("candidate_links:")
        for link in result["candidate_links"][:_MAX_PRINTED_LINKS]:
            typer.echo(f"  - {link}")
        remaining = len(result["candidate_links"]) - _MAX_PRINTED_LINKS
        if remaining > 0:
            typer.echo(f"candidate_links_more: {remaining}")
    else:
        typer.echo("candidate_links: none")


def register(app: typer.Typer) -> None:
    app.command("scout-url")(scout_url)
