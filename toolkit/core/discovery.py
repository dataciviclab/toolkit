"""Config discovery — trova dataset.yml con risoluzione progressiva.

Centralizza la logica oggi dispersa tra:
- ``mcp/path_safety.py``: _resolve_dataset() slug→path per MCP
- CLI: ``--config`` obbligatorio, mai auto-detect

La funzione ``resolve_config_path()`` implementa 3 stadi:
1. CWD o path diretto
2. Slug → candidates/compose/support_datasets
3. FileNotFoundError con suggerimento
"""

from __future__ import annotations

from pathlib import Path

from toolkit.core.paths import WORKSPACE_ROOT

_INCUBATOR_DIRS = ("candidates", "compose", "support_datasets")


def resolve_config_path(
    hint: str | Path | None = None,
    workspace: Path | None = None,
) -> Path:
    """Trova e restituisce il path assoluto a ``dataset.yml``.

    Args:
        hint: ``None`` → cerca ``dataset.yml`` nel CWD.
              Path o stringa con ``/`` o ``.yml`` → path diretto.
              Stringa senza ``/`` né ``.yml`` → slug risolto in
              ``candidates/{slug}/dataset.yml`` (e categorie affini).
        workspace: Workspace root (default: ``WORKSPACE_ROOT``).

    Returns:
        Path assoluto a ``dataset.yml``.

    Raises:
        FileNotFoundError: con suggerimenti.
    """
    ws = (workspace or WORKSPACE_ROOT).resolve()
    cwd = Path.cwd().resolve()

    # ── Stage 1: nessun hint → CWD ────────────────────────────────────
    if hint is None:
        for name in ("dataset.yml", "dataset.yaml"):
            candidate = cwd / name
            if candidate.is_file():
                return candidate.resolve()
        raise FileNotFoundError(
            f"dataset.yml non trovato in {cwd}.\n"
            f"  Spostati in un dataset o passa --config o -c <slug>"
        )

    hint_str = str(hint)
    hint_path = Path(hint).expanduser()

    # ── Stage 2: path diretto (contiene / o .yml/.yaml) ───────────────
    _is_path_like = "/" in hint_str or hint_str.endswith((".yml", ".yaml"))
    if _is_path_like:
        candidate = hint_path if hint_path.is_absolute() else (cwd / hint_path).resolve()
        if candidate.suffix in (".yml", ".yaml") and candidate.is_file():
            return candidate
        if candidate.is_dir():
            for name in ("dataset.yml", "dataset.yaml"):
                p = candidate / name
                if p.is_file():
                    return p
            raise FileNotFoundError(f"{candidate} è una directory ma non contiene dataset.yml")
        raise FileNotFoundError(
            f"File non trovato: {candidate}\n  Usa -c <slug> per risolvere automaticamente"
        )

    # ── Stage 3: risoluzione slug ─────────────────────────────────────
    incubator = ws / "dataset-incubator"
    for subdir in _INCUBATOR_DIRS:
        for name in ("dataset.yml", "dataset.yaml"):
            probe = incubator / subdir / hint_str / name
            if probe.is_file():
                return probe.resolve()

    raise FileNotFoundError(
        f"Nessun dataset trovato per '{hint_str}'.\n"
        f"  Cercato in: {ws}/dataset-incubator/{{candidates,compose,support_datasets}}/<slug>/dataset.yml\n"
        f"  Verifica che lo slug sia corretto."
    )
