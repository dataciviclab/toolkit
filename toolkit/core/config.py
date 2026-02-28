# toolkit/core/config.py
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


logger = logging.getLogger(__name__)
_MANAGED_OUTPUT_ROOTS = {"_smoke_out", "_test_out"}


@dataclass(frozen=True)
class ToolkitConfig:
    base_dir: Path
    schema_version: int
    root: Path
    root_source: str
    dataset: str
    years: list[int]
    raw: dict[str, Any]
    clean: dict[str, Any]
    mart: dict[str, Any]
    validation: dict[str, Any]
    output: dict[str, Any]
    bq: dict[str, Any] | None

    def resolve(self, rel_path: str | Path) -> Path:
        p = Path(rel_path)
        return p if p.is_absolute() else (self.base_dir / p)

    def resolved_root(self) -> Path:
        return self.root


def _err(msg: str, *, path: Path) -> ValueError:
    return ValueError(f"{msg} (file: {path})")


def parse_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    raise ValueError(
        f"{field_name} must be a boolean-like value: true/false, 1/0, yes/no"
    )


def ensure_str_list(value: Any, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        if not all(isinstance(item, str) for item in value):
            raise ValueError(f"{field_name} must be a string or a list of strings")
        return list(value)
    raise ValueError(f"{field_name} must be a string or a list of strings")


def _require_map(data: Mapping[str, Any], key: str, *, path: Path) -> dict[str, Any]:
    val = data.get(key)
    if not isinstance(val, dict):
        raise _err(f"Campo '{key}' mancante o non valido (deve essere una mappa).", path=path)
    return val


_SECTION_PATH_WHITELIST: dict[str, tuple[tuple[str, ...], ...]] = {
    "raw": (
        ("source", "args", "path"),
        ("sources", "*", "args", "path"),
    ),
    "clean": (
        ("sql",),
        ("sql_path",),
    ),
    "mart": (
        ("sql_dir",),
        ("tables", "*", "sql"),
    ),
}


def _resolve_path_value(value: Any, *, base_dir: Path) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value
    path = Path(text).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (base_dir / path).resolve()


def _path_tokens_to_str(tokens: tuple[str, ...]) -> str:
    out: list[str] = []
    for token in tokens:
        if token == "*":
            if out:
                out[-1] = f"{out[-1]}[*]"
            else:
                out.append("[*]")
        elif token.isdigit():
            if out:
                out[-1] = f"{out[-1]}[{token}]"
            else:
                out.append(f"[{token}]")
        else:
            out.append(token)
    return ".".join(out)


def _set_nested_value(container: Any, tokens: tuple[str, ...], value: Any) -> Any:
    if not tokens:
        return value

    head, *tail = tokens
    if isinstance(container, dict):
        updated = dict(container)
        updated[head] = _set_nested_value(updated.get(head), tuple(tail), value)
        return updated

    if isinstance(container, list):
        index = int(head)
        updated = list(container)
        updated[index] = _set_nested_value(updated[index], tuple(tail), value)
        return updated

    raise TypeError(f"Cannot set nested value at {tokens!r} on {type(container).__name__}")


def _iter_matching_tokens(
    container: Any,
    pattern: tuple[str, ...],
    prefix: tuple[str, ...] = (),
) -> list[tuple[str, ...]]:
    if not pattern:
        return [prefix]

    head, *tail = pattern
    tail_tuple = tuple(tail)

    if head == "*":
        if not isinstance(container, list):
            return []
        matches: list[tuple[str, ...]] = []
        for index, item in enumerate(container):
            matches.extend(_iter_matching_tokens(item, tail_tuple, prefix + (str(index),)))
        return matches

    if not isinstance(container, dict) or head not in container:
        return []

    return _iter_matching_tokens(container[head], tail_tuple, prefix + (head,))


def _get_nested_value(container: Any, tokens: tuple[str, ...]) -> Any:
    current = container
    for token in tokens:
        if isinstance(current, dict):
            current = current[token]
        elif isinstance(current, list):
            current = current[int(token)]
        else:
            raise TypeError(f"Cannot traverse token {token!r} on {type(current).__name__}")
    return current


def _normalize_section_paths(
    section_name: str,
    section: dict[str, Any],
    *,
    base_dir: Path,
) -> tuple[dict[str, Any], list[tuple[str, Path]]]:
    normalized = dict(section)
    changes: list[tuple[str, Path]] = []

    for pattern in _SECTION_PATH_WHITELIST.get(section_name, ()):
        for tokens in _iter_matching_tokens(section, pattern):
            raw_value = _get_nested_value(section, tokens)
            resolved = _resolve_path_value(raw_value, base_dir=base_dir)
            if resolved is raw_value:
                continue
            normalized = _set_nested_value(normalized, tokens, resolved)
            changes.append((f"{section_name}.{_path_tokens_to_str(tokens)}", resolved))

    return normalized, changes


def _normalize_clean_read_legacy(clean: dict[str, Any], *, path: Path) -> dict[str, Any]:
    if not isinstance(clean, dict):
        return clean

    read_cfg = clean.get("read")
    if not isinstance(read_cfg, dict):
        return clean

    csv_cfg = read_cfg.get("csv")
    if csv_cfg is None:
        return clean
    if not isinstance(csv_cfg, dict):
        raise _err("clean.read.csv deve essere una mappa YAML (oggetto).", path=path)

    normalized_read = dict(read_cfg)
    normalized_read.pop("csv", None)
    for key, value in csv_cfg.items():
        normalized_read.setdefault(key, value)

    logger.warning(
        "Deprecated config keys: clean.read.csv.* is deprecated and will be removed in a future release. "
        "Migrate to clean.read.source / clean.read.columns and move CSV options directly under clean.read."
    )

    return {
        **clean,
        "read": normalized_read,
    }


def _normalize_raw_booleans(raw: dict[str, Any], *, path: Path) -> dict[str, Any]:
    normalized = dict(raw)

    def _normalize_primary(container: dict[str, Any], field_name: str) -> dict[str, Any]:
        updated = dict(container)
        if "primary" in updated:
            updated["primary"] = parse_bool(updated["primary"], field_name)
        return updated

    source = normalized.get("source")
    if isinstance(source, dict):
        normalized["source"] = _normalize_primary(source, "raw.source.primary")

    sources = normalized.get("sources")
    if isinstance(sources, list):
        updated_sources: list[Any] = []
        for index, source_entry in enumerate(sources):
            if isinstance(source_entry, dict):
                updated_sources.append(
                    _normalize_primary(source_entry, f"raw.sources[{index}].primary")
                )
            else:
                updated_sources.append(source_entry)
        normalized["sources"] = updated_sources

    return normalized


def _normalize_clean_validation(clean: dict[str, Any], *, path: Path) -> dict[str, Any]:
    normalized = dict(clean)

    if "sql_path" in normalized:
        logger.warning("Deprecated/unused config field detected: clean.sql_path is ignored.")

    if "required_columns" in normalized:
        normalized["required_columns"] = ensure_str_list(
            normalized.get("required_columns"),
            "clean.required_columns",
        )

    validate_cfg = normalized.get("validate")
    if isinstance(validate_cfg, dict):
        updated_validate = dict(validate_cfg)
        for key in ("primary_key", "not_null"):
            if key in updated_validate:
                updated_validate[key] = ensure_str_list(
                    updated_validate.get(key),
                    f"clean.validate.{key}",
                )
        normalized["validate"] = updated_validate

    return normalized


def _normalize_mart_validation(mart: dict[str, Any], *, path: Path) -> dict[str, Any]:
    normalized = dict(mart)

    if "sql_dir" in normalized:
        logger.warning("Deprecated/unused config field detected: mart.sql_dir is ignored.")

    if "required_tables" in normalized:
        normalized["required_tables"] = ensure_str_list(
            normalized.get("required_tables"),
            "mart.required_tables",
        )

    validate_cfg = normalized.get("validate")
    if isinstance(validate_cfg, dict):
        updated_validate = dict(validate_cfg)
        table_rules = updated_validate.get("table_rules")
        if isinstance(table_rules, dict):
            updated_rules: dict[str, Any] = {}
            for table_name, rule in table_rules.items():
                if isinstance(rule, dict):
                    updated_rule = dict(rule)
                    for key in ("required_columns", "not_null", "primary_key"):
                        if key in updated_rule:
                            updated_rule[key] = ensure_str_list(
                                updated_rule.get(key),
                                f"mart.validate.table_rules.{table_name}.{key}",
                            )
                    updated_rules[table_name] = updated_rule
                else:
                    updated_rules[table_name] = rule
            updated_validate["table_rules"] = updated_rules
        normalized["validate"] = updated_validate

    return normalized


def _is_managed_output_root(root: str) -> bool:
    raw = root.strip()
    if not raw:
        return False
    path = Path(raw)
    if path.is_absolute():
        return False
    return path.name in _MANAGED_OUTPUT_ROOTS


def load_config(path: str | Path) -> ToolkitConfig:
    p = Path(path)
    base_dir = p.parent.resolve()

    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise _err(f"Impossibile leggere YAML: {e}", path=p)

    if not isinstance(data, dict):
        raise _err("dataset.yml deve essere una mappa YAML.", path=p)

    schema_version = int(data.get("schema_version", 1))

    dataset_block = _require_map(data, "dataset", path=p)
    name = dataset_block.get("name")
    if not name or not isinstance(name, str):
        raise _err("Campo obbligatorio mancante o non valido: dataset.name (string).", path=p)

    years = dataset_block.get("years")
    if not isinstance(years, (list, tuple)) or not years:
        raise _err("dataset.years deve essere una lista non vuota, es: [2022, 2023].", path=p)
    try:
        years_int = [int(y) for y in years]
    except Exception:
        raise _err("dataset.years deve contenere solo numeri (es: [2022, 2023]).", path=p)

    raw = data.get("raw", {}) or {}
    clean = data.get("clean", {}) or {}
    mart = data.get("mart", {}) or {}
    validation = data.get("validation", {}) or {}
    output = data.get("output", {}) or {}
    if not isinstance(raw, dict) or not isinstance(clean, dict) or not isinstance(mart, dict):
        raise _err("raw/clean/mart devono essere mappe YAML (oggetti).", path=p)
    if not isinstance(validation, dict):
        raise _err("validation deve essere una mappa YAML (oggetto).", path=p)
    if not isinstance(output, dict):
        raise _err("output deve essere una mappa YAML (oggetto).", path=p)

    if "bq" in data:
        logger.warning("Unused config field detected: bq is currently ignored by the toolkit.")

    clean = _normalize_clean_read_legacy(clean, path=p)
    raw = _normalize_raw_booleans(raw, path=p)
    clean = _normalize_clean_validation(clean, path=p)
    mart = _normalize_mart_validation(mart, path=p)

    validation = {
        **validation,
        "fail_on_error": parse_bool(
            validation.get("fail_on_error", True),
            "validation.fail_on_error",
        ),
    }
    output = {
        **output,
        "artifacts": output.get("artifacts", "standard"),
        "legacy_aliases": parse_bool(
            output.get("legacy_aliases", True),
            "output.legacy_aliases",
        ),
    }

    root = data.get("root")
    if isinstance(root, str) and root.strip():
        managed_outdir = os.environ.get("TOOLKIT_OUTDIR") or os.environ.get("DCL_OUTDIR")
        if managed_outdir and _is_managed_output_root(root):
            root_path = Path(managed_outdir).expanduser().resolve()
            root_source = "env:TOOLKIT_OUTDIR" if os.environ.get("TOOLKIT_OUTDIR") else "env:DCL_OUTDIR"
        else:
            root_path = _resolve_path_value(root, base_dir=base_dir)
            root_source = "yml"
    else:
        env_root = os.environ.get("DCL_ROOT")
        if env_root:
            root_path = Path(env_root).expanduser().resolve()
            root_source = "env:DCL_ROOT"
        else:
            root_path = base_dir
            root_source = "base_dir_fallback"

    bq = data.get("bq")
    if bq is not None and not isinstance(bq, dict):
        raise _err("bq deve essere una mappa YAML (oggetto) oppure null.", path=p)

    normalized_fields: list[tuple[str, Path]] = []
    raw_normalized, raw_changes = _normalize_section_paths("raw", raw, base_dir=base_dir)
    clean_normalized, clean_changes = _normalize_section_paths("clean", clean, base_dir=base_dir)
    mart_normalized, mart_changes = _normalize_section_paths("mart", mart, base_dir=base_dir)
    normalized_fields.extend(raw_changes)
    normalized_fields.extend(clean_changes)
    normalized_fields.extend(mart_changes)
    normalized_fields.append(("root", root_path))

    if normalized_fields:
        summary = ", ".join(f"{field}={value}" for field, value in normalized_fields)
        logger.debug("Normalized config paths: %s", summary)

    return ToolkitConfig(
        base_dir=base_dir,
        schema_version=schema_version,
        root=root_path,
        root_source=root_source,
        dataset=name,
        years=years_int,
        raw=raw_normalized,
        clean=clean_normalized,
        mart=mart_normalized,
        validation=validation,
        output=output,
        bq=bq,
    )
