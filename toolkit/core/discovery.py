"""Config discovery — trova dataset.yml con risoluzione progressiva.

Centralizza la logica oggi dispersa tra:
- ``mcp/path_safety.py``: _resolve_dataset() slug→path per MCP
- ``domain/catalog.py``: _scan_workspace_configs() scansione bulk
- ``dataset-incubator/scripts/notebook_helpers.py``: find_config() CWD climbing
- CLI: ``--config`` obbligatorio, mai auto-detect

La funzione principale ``resolve_config_path()`` implementa 4 stadi di
risoluzione progressiva. Ogni stadio resta autonomo: se fallisce passa
al successivo senza eccezioni intermedie.
"""

from __future__ import annotations

from pathlib import Path

from toolkit.core.paths import WORKSPACE_ROOT

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

CONFIG_FILENAMES = frozenset({"dataset.yml", "dataset.yaml"})

_INCUBATOR_DIRS: tuple[str, ...] = (
    "candidates",
    "compose",
    "support_datasets",
)

_MAX_CLIMB_DEPTH = 20


# ---------------------------------------------------------------------------
# Helpers interni
# ---------------------------------------------------------------------------


def _is_config_filename(name: str) -> bool:
    return name in CONFIG_FILENAMES


def _search_parents(start: Path) -> Path | None:
    """Risale da *start* verso la radice, cercando un file di config.

    Si ferma al primo genitore che contiene una directory ``.git``
    (repo boundary) — evita di risalire oltre il repository corrente.
    Se il repo-root stesso contiene ``dataset.yml``, lo restituisce
    prima di fermarsi.
    """
    probe = start.resolve()
    for _ in range(_MAX_CLIMB_DEPTH):
        for name in CONFIG_FILENAMES:
            candidate = probe / name
            if candidate.is_file():
                return candidate.resolve()
        # Stop: genitore con .git e senza dataset.yml = repo boundary
        if (probe / ".git").exists():
            return None
        # Stop: filesystem root
        parent = probe.parent
        if parent == probe:
            return None
        probe = parent
    return None  # superata profondità massima, safety


def _slug_lookup(slug: str, workspace: Path) -> Path | None:
    """Cerca uno slug nelle directory dataset-incubator del workspace.

    Ordine: candidates → compose → support_datasets → glob fallback.
    """
    incubator = workspace / "dataset-incubator"
    if not incubator.is_dir():
        return None

    # Lookup diretto per categoria
    for subdir in _INCUBATOR_DIRS:
        probe = incubator / subdir / slug / "dataset.yml"
        if probe.is_file():
            return probe.resolve()
        probe_yaml = incubator / subdir / slug / "dataset.yaml"
        if probe_yaml.is_file():
            return probe_yaml.resolve()

    # Path annidato (es. slug = "istat-housing/sources/a_base")
    for subdir in _INCUBATOR_DIRS:
        base = incubator / subdir
        for name in CONFIG_FILENAMES:
            probe = base / slug / name
            if probe.is_file():
                return probe.resolve()
            # Se slug termina già con .yml/.yaml
            if slug.endswith((".yml", ".yaml")):
                probe2 = base / slug
                if probe2.is_file():
                    return probe2.resolve()

    # Glob ricorsivo come fallback
    matches: list[Path] = []
    for subdir in _INCUBATOR_DIRS:
        base = incubator / subdir
        if not base.is_dir():
            continue
        matches.extend(sorted(base.rglob(f"**/{slug}/dataset.yml")))
        if not matches:
            matches.extend(sorted(base.rglob(f"**/{slug}/dataset.yaml")))
        if not matches:
            matches.extend(sorted(base.rglob(f"**/{slug}.yml")))
        if matches:
            return matches[0].resolve()

    return None


def _format_search_log(
    hint: str | None,
    cwd: Path,
    stages_reached: list[str],
) -> str:
    """Formatta un messaggio di errore leggibile con ciò che è stato cercato."""
    lines = [
        "dataset.yml non trovato.",
        "",
        "Cercato in:",
    ]
    for stage in stages_reached:
        lines.append(f"  {stage}")
    lines.append("")
    lines.append(f"Directory corrente: {cwd}")
    if hint:
        lines.append(f"Hint ricevuto: {hint}")
    lines.append("")
    lines.append("Suggerimenti:")
    lines.append("  - Spostati nella directory del dataset:")
    lines.append("      cd candidates/<slug>/")
    lines.append("      toolkit run all")
    lines.append("  - Specifica il percorso esplicito:")
    lines.append("      toolkit run all -c candidates/<slug>/dataset.yml")
    lines.append("  - Usa l'auto-detect per slug:")
    lines.append("      toolkit run all -c <slug>")
    lines.append("  - Verifica che il dataset esista in:")
    lines.append("      dataset-incubator/candidates/<slug>/dataset.yml")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# API pubblica
# ---------------------------------------------------------------------------


def resolve_config_path(
    hint: str | Path | None = None,
    workspace: Path | None = None,
) -> Path:
    """Trova e restituisce il path assoluto a ``dataset.yml``.

    Implementa risoluzione progressiva a 4 stadi. Ogni stadio è autonomo:
    se non trova, passa al successivo. L'ultimo stadio produce un
    ``FileNotFoundError`` leggibile con il log della ricerca.

    Args:
        hint: Può essere:
            - ``None``: auto-detect (CWD, poi risalita directory)
            - Path o stringa con ``/`` o ``.yml``: path diretto al file
              o directory che contiene ``dataset.yml``
            - Stringa senza ``/`` né ``.yml``: slug risolto in
              ``candidates/{slug}/dataset.yml`` (e categorie affini)
        workspace: Workspace root (default: ``WORKSPACE_ROOT`` da
            ``core/paths.py``, tipicamente il parent di ``toolkit/``)

    Returns:
        Path assoluto e risolto al file ``dataset.yml``.

    Raises:
        FileNotFoundError: con dettaglio di ciò che è stato cercato e
            suggerimenti.
    """
    ws = (workspace or WORKSPACE_ROOT).resolve()
    cwd = Path.cwd().resolve()
    stages_reached: list[str] = []

    # ── Stage 0: nessun hint — cerca in CWD e risali ────────────────────
    if hint is None:
        stages_reached.append(f"  ./dataset.yml (CWD: {cwd})")
        for name in CONFIG_FILENAMES:
            candidate = cwd / name
            if candidate.is_file():
                return candidate.resolve()

        stages_reached.append("  risalita directory (fino a repo boundary)")
        found = _search_parents(cwd)
        if found is not None:
            return found

        stages_reached.append(f"  slug lookup in {ws}/dataset-incubator/...")
        # Nessun hint: prova a usare il nome della directory CWD come slug
        cwd_slug = cwd.name
        found = _slug_lookup(cwd_slug, ws)
        if found is not None:
            return found

        raise FileNotFoundError(_format_search_log(None, cwd, stages_reached))

    # Normalizza hint
    hint_str = str(hint)
    hint_path = Path(hint).expanduser()

    # ── Stage 1: path diretto (contiene / o .yml/.yaml) ─────────────────
    _is_path_like = "/" in hint_str or hint_str.endswith((".yml", ".yaml"))
    if _is_path_like:
        stages_reached.append(f"  {hint_str} (path diretto)")

        # Prova risoluzione rispetto a CWD
        candidate_path = hint_path
        if not candidate_path.is_absolute():
            candidate_path = (Path.cwd() / hint_path).resolve()
        else:
            candidate_path = candidate_path.resolve()

        # File diretto
        if candidate_path.suffix in (".yml", ".yaml") and candidate_path.is_file():
            return candidate_path

        # Directory con dataset.yml dentro
        if candidate_path.is_dir():
            for name in CONFIG_FILENAMES:
                candidate = candidate_path / name
                if candidate.is_file():
                    return candidate
            # Directory valida ma senza dataset.yml → errore subito
            raise FileNotFoundError(
                f"'{candidate_path}' è una directory ma non contiene "
                f"dataset.yml o dataset.yaml.\n"
                f"Usa --config per specificare il path esatto."
            )

        # Path non trovato — errore chiaro (non cascare a slug con un path)
        raise FileNotFoundError(
            f"File non trovato: {candidate_path}\n"
            f"Hint ricevuto: {hint_str}\n"
            f"Verifica che il path sia corretto o usa uno slug.\n"
            f"  toolkit run all -c <slug>\n"
            f"  toolkit run all  (auto-detect da CWD)"
        )

    # ── Stage 2: risoluzione slug ────────────────────────────────────────
    slug = hint_str
    stages_reached.append(
        f"  {ws}/dataset-incubator/{{candidates,compose,support}}/{slug}/dataset.yml"
    )
    found = _slug_lookup(slug, ws)
    if found is not None:
        return found

    # ── Stage 4: FileNotFoundError con riepilogo ─────────────────────────
    raise FileNotFoundError(_format_search_log(hint_str, cwd, stages_reached))


def list_workspace_configs(
    workspace: Path | None = None,
    stage: str = "all",
) -> list[Path]:
    """Restituisce tutti i path a ``dataset.yml`` nel workspace.

    Args:
        workspace: Workspace root (default: ``WORKSPACE_ROOT``).
        stage: Filtro categoria: ``"candidates"``, ``"compose"``,
            ``"support"``, ``"all"`` (default).

    Returns:
        Lista ordinata di path assoluti a ``dataset.yml``.
    """
    ws = (workspace or WORKSPACE_ROOT).resolve()
    incubator = ws / "dataset-incubator"
    if not incubator.is_dir():
        return []

    results: list[Path] = []

    if stage in ("candidates", "all"):
        candidates_dir = incubator / "candidates"
        if candidates_dir.is_dir():
            results.extend(sorted(candidates_dir.rglob("dataset.yml")))
            results.extend(sorted(candidates_dir.rglob("dataset.yaml")))

    if stage in ("compose", "all"):
        compose_dir = incubator / "compose"
        if compose_dir.is_dir():
            results.extend(sorted(compose_dir.rglob("dataset.yml")))
            results.extend(sorted(compose_dir.rglob("dataset.yaml")))

    if stage in ("support", "all"):
        support_dir = incubator / "support_datasets"
        if support_dir.is_dir():
            results.extend(sorted(support_dir.rglob("dataset.yml")))
            results.extend(sorted(support_dir.rglob("dataset.yaml")))

    # Filtra template
    results = [p.resolve() for p in results if "templates" not in p.parts]
    return sorted(set(results))
