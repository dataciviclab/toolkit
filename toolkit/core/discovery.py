"""Config discovery — trova dataset.yml con risoluzione progressiva.

Centralizza la logica oggi dispersa tra:
- ``mcp/path_safety.py``: _resolve_dataset() slug→path per MCP
- CLI: ``--config`` obbligatorio, mai auto-detect

La funzione ``resolve_config_path()`` implementa 3 stadi:
1. CWD o path diretto
2. Slug → repo del workspace con la mappa canonica ``REPO_DATASET_DIRS``
   (dataset-incubator: candidates/compose/support_datasets; altri repo: datasets)
3. FileNotFoundError con suggerimento
"""

from __future__ import annotations

from pathlib import Path

from toolkit.core.paths import WORKSPACE_ROOT


def resolve_config_path(
    hint: str | Path | None = None,
    workspace: Path | None = None,
) -> Path:
    """Trova e restituisce il path assoluto a ``dataset.yml``.

    Args:
        hint: ``None`` → cerca ``dataset.yml`` nel CWD.
              Path o stringa con ``/`` o ``.yml`` → path diretto.
              Stringa senza ``/`` né ``.yml`` → slug risolto nei repo del
              workspace (dataset-incubator candidates/compose/support_datasets,
              altri repo datasets/).
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

    # ── Stage 3: risoluzione slug nei repo del workspace ──────────────
    from toolkit.registry.layout import repo_dataset_dirs

    # Lo slug canonico è dataset.name (underscore); la dir è un contenitore
    # libero (hyphen). Prova entrambe le forme per coprire dir≠slug.
    hint_forms = {hint_str, hint_str.replace("_", "-")}

    searched: list[str] = []
    for repo_dir in sorted(p for p in ws.iterdir() if p.is_dir()):
        for section in repo_dataset_dirs(repo_dir.name):
            section_dir = repo_dir / section
            if not section_dir.is_dir():
                continue
            for form in sorted(hint_forms):
                for name in ("dataset.yml", "dataset.yaml"):
                    probe = section_dir / form / name
                    if probe.is_file():
                        return probe.resolve()
                searched.append(str(section_dir / form))

    raise FileNotFoundError(
        f"Nessun dataset trovato per '{hint_str}'.\n"
        f"  Cercato in: {', '.join(searched) or ws}\n"
        f"  Verifica che lo slug sia corretto."
    )
