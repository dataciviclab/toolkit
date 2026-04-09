from __future__ import annotations

import json
from pathlib import Path

from toolkit.core.manifest import read_raw_manifest
from toolkit.core.paths import from_root_relative, layer_year_dir, resolve_root
from toolkit.core.run_context import get_run_dir, list_runs


def is_supported_input_file(path: Path) -> bool:
    name = path.name.lower()
    if name.endswith((".json", ".md", ".yml", ".yaml")):
        return False
    if name.endswith((".csv.gz", ".tsv.gz", ".txt.gz", ".nt.gz")):
        return True
    if path.suffix.lower() in {".csv", ".tsv", ".txt", ".parquet", ".xlsx", ".nt"}:
        return True
    return False


def list_input_files(raw_dir: Path, glob: str = "*") -> list[Path]:
    pattern = glob or "*"
    return sorted(
        [
            path
            for path in raw_dir.glob(pattern)
            if path.is_file() and is_supported_input_file(path) and path.stat().st_size > 0
        ],
        key=lambda item: item.name.lower(),
    )


def _sorted_unique_paths(paths: list[Path]) -> list[Path]:
    return sorted(set(paths), key=lambda item: item.name.lower())


def _match_patterns(paths: list[Path], patterns: list[str]) -> list[Path]:
    matched: list[Path] = []
    for path in paths:
        if any(path.match(pattern) or path.name == pattern for pattern in patterns):
            matched.append(path)
    return _sorted_unique_paths(matched)


def _metadata_candidates(raw_dir: Path) -> list[Path]:
    metadata_path = raw_dir / "metadata.json"
    if not metadata_path.exists():
        return []

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    file_names: list[str] = []
    for key in ("files", "outputs"):
        for entry in metadata.get(key, []):
            if isinstance(entry, dict) and entry.get("file"):
                file_names.append(str(entry["file"]))

    candidates: list[Path] = []
    for name in file_names:
        candidate = raw_dir / name
        if _is_usable_input_file(candidate):
            candidates.append(candidate)
    return _sorted_unique_paths(candidates)


def _is_usable_input_file(path: Path) -> bool:
    return (
        path.exists()
        and path.is_file()
        and is_supported_input_file(path)
        and path.stat().st_size > 0
    )


def _run_record_paths(root: str | None, dataset: str, year: int) -> list[Path]:
    return list_runs(get_run_dir(resolve_root(root), dataset, year))


def _raw_layer_succeeded(run_path: Path) -> bool:
    record = json.loads(run_path.read_text(encoding="utf-8"))
    raw_layer = (record.get("layers") or {}).get("raw") or {}
    return raw_layer.get("status") == "SUCCESS"


def _latest_raw_success_exists(root: str | None, dataset: str, year: int) -> bool:
    runs = _run_record_paths(root, dataset, year)
    for run_path in sorted(runs, key=lambda item: item.stat().st_mtime, reverse=True):
        if _raw_layer_succeeded(run_path):
            return True
    return False


def list_raw_candidates(
    root: str | None,
    dataset: str,
    year: int,
    glob: str = "*",
    prefer_from_raw_run: bool = True,
) -> list[Path]:
    raw_dir = layer_year_dir(root, "raw", dataset, year)
    if not raw_dir.exists():
        return []

    pattern = glob or "*"
    candidates = list_input_files(raw_dir, pattern)

    if not prefer_from_raw_run:
        return candidates

    if not _latest_raw_success_exists(root, dataset, year):
        return candidates

    metadata_based = _metadata_candidates(raw_dir)
    if not metadata_based:
        return candidates

    filtered = _match_patterns(metadata_based, [pattern])
    return filtered or candidates


def _manifest_primary_input(raw_year_dir: Path) -> tuple[Path | None, str | None]:
    manifest = read_raw_manifest(raw_year_dir)
    if not manifest:
        return None, "CLEAN RAW manifest missing, using legacy selection."

    primary_output_file = manifest.get("primary_output_file")
    if not isinstance(primary_output_file, str) or not primary_output_file.strip():
        return None, "CLEAN RAW manifest missing primary_output_file; using legacy selection."

    manifest_path = from_root_relative(primary_output_file, raw_year_dir)
    if _is_usable_input_file(manifest_path):
        return manifest_path, None

    return (
        None,
        "CLEAN RAW manifest primary_output_file is missing or invalid: "
        f"{primary_output_file}; using legacy selection.",
    )


def _should_use_manifest_primary(mode: str, include=None) -> bool:
    if mode == "all":
        return False
    if mode == "explicit" and include is not None:
        return False
    return True


def select_raw_input(
    raw_year_dir: Path,
    logger,
    *,
    mode: str,
    root: str | None = None,
    dataset: str | None = None,
    year: int | None = None,
    glob: str = "*",
    prefer_from_raw_run: bool = True,
    candidates: list[Path] | None = None,
    include=None,
    allow_ambiguous: bool = False,
) -> list[Path]:
    selected_candidates = list(candidates or [])
    if not selected_candidates:
        if dataset is None or year is None:
            selected_candidates = list_input_files(raw_year_dir, glob)
        else:
            selected_candidates = list_raw_candidates(
                root,
                dataset,
                year,
                glob=glob,
                prefer_from_raw_run=prefer_from_raw_run,
            )

    if _should_use_manifest_primary(mode, include=include):
        manifest_primary, manifest_warning = _manifest_primary_input(raw_year_dir)
        if manifest_primary is not None:
            return [manifest_primary]
        if manifest_warning is not None:
            logger.warning(manifest_warning)

    selected = select_inputs(
        selected_candidates,
        mode,
        include=include,
        allow_ambiguous=allow_ambiguous,
    )
    if not selected:
        raise FileNotFoundError(
            f"No usable RAW files found in {raw_year_dir} for legacy CLEAN selection."
        )
    return selected


def select_inputs(
    candidates: list[Path],
    mode: str,
    include=None,
    allow_ambiguous: bool = False,
) -> list[Path]:
    candidates = sorted(candidates, key=lambda item: item.name.lower())
    if not candidates:
        return []

    if mode == "explicit":
        if include is None:
            raise ValueError("clean.read.mode=explicit requires clean.read.include")
        patterns = [include] if isinstance(include, str) else list(include)
        selected = _match_patterns(candidates, patterns)
        if not selected:
            raise FileNotFoundError(
                "No CLEAN input files matched clean.read.include: "
                f"{patterns}. Available: {[path.name for path in candidates]}"
            )
        if len(selected) > 1 and not allow_ambiguous:
            raise ValueError(
                "clean.read.mode=explicit matched multiple files. "
                "Set clean.read.allow_ambiguous: true or narrow clean.read.include. "
                f"Matched: {[path.name for path in selected]}"
            )
        return selected

    if mode == "latest":
        return [max(candidates, key=lambda item: (item.stat().st_mtime, item.name.lower()))]

    if mode == "largest":
        return [max(candidates, key=lambda item: (item.stat().st_size, item.name.lower()))]

    if mode == "all":
        return candidates

    raise ValueError(f"Unsupported clean.read.mode: {mode}")
