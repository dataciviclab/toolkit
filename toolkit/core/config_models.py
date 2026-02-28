from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from toolkit.core.csv_read import normalize_columns_spec


logger = logging.getLogger("toolkit.core.config")
_MANAGED_OUTPUT_ROOTS = {"_smoke_out", "_test_out"}


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


class DatasetBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    years: list[int]


class OutputConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifacts: Literal["minimal", "standard", "debug"] = "standard"
    legacy_aliases: bool = True

    @field_validator("legacy_aliases", mode="before")
    @classmethod
    def _parse_legacy_aliases(cls, value: Any) -> bool:
        return parse_bool(value, "output.legacy_aliases")


class GlobalValidationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fail_on_error: bool = True

    @field_validator("fail_on_error", mode="before")
    @classmethod
    def _parse_fail_on_error(cls, value: Any) -> bool:
        return parse_bool(value, "validation.fail_on_error")


class ExtractorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["identity", "unzip_all", "unzip_first", "unzip_first_csv"] = "identity"
    args: dict[str, Any] = Field(default_factory=dict)


class RawSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    type: str = "http_file"
    client: dict[str, Any] = Field(default_factory=dict)
    args: dict[str, Any] = Field(default_factory=dict)
    extractor: ExtractorConfig | None = None
    primary: bool = False

    @field_validator("primary", mode="before")
    @classmethod
    def _parse_primary(cls, value: Any) -> bool:
        return parse_bool(value, "raw.sources[].primary")


class RawConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    output_policy: Literal["overwrite", "versioned"] = "versioned"
    extractor: ExtractorConfig | None = None
    sources: list[RawSourceConfig] = Field(default_factory=list)


class CleanReadConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["auto", "config_only"] = "auto"
    delim: str | None = None
    header: bool = True
    encoding: str | None = None
    decimal: str | None = None
    skip: int | None = None
    auto_detect: bool | None = None
    quote: str | None = None
    escape: str | None = None
    comment: str | None = None
    ignore_errors: bool | None = None
    strict_mode: bool | None = None
    null_padding: bool | None = None
    nullstr: str | list[str] | None = None
    columns: dict[str, str] | None = None
    trim_whitespace: bool = True
    sample_size: int | None = None
    mode: Literal["explicit", "latest", "largest", "all"] | None = None
    glob: str = "*"
    prefer_from_raw_run: bool = True
    allow_ambiguous: bool = False
    include: list[str] | None = None

    @field_validator("columns", mode="before")
    @classmethod
    def _normalize_columns(cls, value: Any) -> dict[str, str] | None:
        return normalize_columns_spec(value)

    @field_validator("include", mode="before")
    @classmethod
    def _normalize_include(cls, value: Any) -> list[str] | None:
        if value is None:
            return None
        return ensure_str_list(value, "clean.read.include")


class CleanValidateConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    primary_key: list[str] = Field(default_factory=list)
    not_null: list[str] = Field(default_factory=list)
    ranges: dict[str, dict[str, float]] = Field(default_factory=dict)
    max_null_pct: dict[str, float] = Field(default_factory=dict)
    min_rows: int | None = None

    @field_validator("primary_key", "not_null", mode="before")
    @classmethod
    def _normalize_lists(cls, value: Any, info) -> list[str]:
        return ensure_str_list(value, f"clean.validate.{info.field_name}")


class CleanMappingSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    from_: str = Field(alias="from")
    type: Literal["int", "integer", "float", "double", "str", "string", "date"] = "str"
    normalize: list[str] | None = None
    nullify: list[str] | None = None
    replace: dict[str, str] | None = None
    parse: dict[str, Any] | None = None


class CleanConfig(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    sql: Path | None = None
    read_mode: Literal["strict", "fallback", "robust"] = "fallback"
    read_source: Literal["auto", "config_only"] | None = None
    read: CleanReadConfig | None = None
    mapping: dict[str, CleanMappingSpec] | None = None
    derive: dict[str, dict[str, str]] | None = None
    required_columns: list[str] = Field(default_factory=list)
    validate_config: CleanValidateConfig = Field(
        default_factory=CleanValidateConfig,
        alias="validate",
    )

    @field_validator("required_columns", mode="before")
    @classmethod
    def _normalize_required_columns(cls, value: Any) -> list[str]:
        return ensure_str_list(value, "clean.required_columns")

    @property
    def validate(self) -> CleanValidateConfig:
        return self.validate_config


class MartTableConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    sql: Path


class MartTableRuleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required_columns: list[str] = Field(default_factory=list)
    not_null: list[str] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    ranges: dict[str, dict[str, float]] = Field(default_factory=dict)
    min_rows: int | None = None

    @field_validator("required_columns", "not_null", "primary_key", mode="before")
    @classmethod
    def _normalize_lists(cls, value: Any, info) -> list[str]:
        return ensure_str_list(value, f"mart.validate.table_rules.*.{info.field_name}")


class MartValidateConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    table_rules: dict[str, MartTableRuleConfig] = Field(default_factory=dict)


class MartConfig(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    tables: list[MartTableConfig] = Field(default_factory=list)
    required_tables: list[str] = Field(default_factory=list)
    validate_config: MartValidateConfig = Field(
        default_factory=MartValidateConfig,
        alias="validate",
    )

    @field_validator("required_tables", mode="before")
    @classmethod
    def _normalize_required_tables(cls, value: Any) -> list[str]:
        return ensure_str_list(value, "mart.required_tables")

    @property
    def validate(self) -> MartValidateConfig:
        return self.validate_config


class ToolkitConfigModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    base_dir: Path
    schema_version: int = 1
    root: Path
    root_source: str
    dataset: DatasetBlock
    raw: RawConfig = Field(default_factory=RawConfig)
    clean: CleanConfig = Field(default_factory=CleanConfig)
    mart: MartConfig = Field(default_factory=MartConfig)
    validation: GlobalValidationConfig = Field(default_factory=GlobalValidationConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    bq: dict[str, Any] | None = None


def _err(msg: str, *, path: Path) -> ValueError:
    return ValueError(f"{msg} (file: {path})")


def _require_map(data: dict[str, Any], key: str, *, path: Path) -> dict[str, Any]:
    val = data.get(key)
    if not isinstance(val, dict):
        raise _err(f"Campo '{key}' mancante o non valido (deve essere una mappa).", path=path)
    return val


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


def _is_managed_output_root(root: str) -> bool:
    raw = root.strip()
    if not raw:
        return False
    path = Path(raw)
    if path.is_absolute():
        return False
    return path.name in _MANAGED_OUTPUT_ROOTS


def _resolve_root(root: Any, *, base_dir: Path) -> tuple[Path, str]:
    if root is None:
        env_root = os.environ.get("DCL_ROOT")
        if env_root:
            return Path(env_root).expanduser().resolve(), "env:DCL_ROOT"
        return base_dir, "base_dir_fallback"

    if not isinstance(root, str):
        raise ValueError("root must be a string path or null")

    if not root.strip():
        env_root = os.environ.get("DCL_ROOT")
        if env_root:
            return Path(env_root).expanduser().resolve(), "env:DCL_ROOT"
        return base_dir, "base_dir_fallback"

    managed_outdir = os.environ.get("TOOLKIT_OUTDIR") or os.environ.get("DCL_OUTDIR")
    if managed_outdir and _is_managed_output_root(root):
        source = "env:TOOLKIT_OUTDIR" if os.environ.get("TOOLKIT_OUTDIR") else "env:DCL_OUTDIR"
        return Path(managed_outdir).expanduser().resolve(), source
    return _resolve_path_value(root, base_dir=base_dir), "yml"


def _normalize_legacy_source(source: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(source)
    plugin = normalized.pop("plugin", None)
    if plugin is not None and "type" not in normalized:
        normalized["type"] = plugin
    source_id = normalized.pop("id", None)
    if source_id is not None and "name" not in normalized:
        normalized["name"] = source_id
    return normalized


def _normalize_legacy_clean_read(clean: dict[str, Any], *, path: Path) -> dict[str, Any]:
    normalized = dict(clean)
    read_cfg = normalized.get("read")

    if isinstance(read_cfg, str):
        logger.warning("Deprecated config form: clean.read scalar form is deprecated; use clean.read.source")
        normalized["read"] = {"source": read_cfg}
        read_cfg = normalized["read"]

    if not isinstance(read_cfg, dict):
        return normalized

    csv_cfg = read_cfg.get("csv")
    if csv_cfg is None:
        return normalized
    if not isinstance(csv_cfg, dict):
        raise _err("clean.read.csv deve essere una mappa YAML (oggetto).", path=path)

    merged_read = dict(read_cfg)
    merged_read.pop("csv", None)
    for key, value in csv_cfg.items():
        merged_read.setdefault(key, value)

    logger.warning(
        "Deprecated config keys: clean.read.csv.* is deprecated and will be removed in a future release. "
        "Migrate to clean.read.source / clean.read.columns and move CSV options directly under clean.read."
    )
    normalized["read"] = merged_read
    return normalized


def _normalize_legacy_payload(data: dict[str, Any], *, path: Path) -> dict[str, Any]:
    normalized = dict(data)

    raw = normalized.get("raw")
    if isinstance(raw, dict):
        updated_raw = dict(raw)
        if "source" in updated_raw:
            source = updated_raw.pop("source")
            if "sources" in updated_raw:
                raise _err("Use either raw.source or raw.sources, not both.", path=path)
            updated_raw["sources"] = [source]
            logger.warning("Deprecated config key: raw.source is deprecated; use raw.sources.")
        sources = updated_raw.get("sources")
        if isinstance(sources, list):
            updated_raw["sources"] = [
                _normalize_legacy_source(source) if isinstance(source, dict) else source
                for source in sources
            ]
        normalized["raw"] = updated_raw

    clean = normalized.get("clean")
    if isinstance(clean, dict):
        updated_clean = _normalize_legacy_clean_read(clean, path=path)
        if "sql_path" in updated_clean:
            logger.warning("Deprecated/unused config field detected: clean.sql_path is ignored.")
        normalized["clean"] = updated_clean

    mart = normalized.get("mart")
    if isinstance(mart, dict):
        if "sql_dir" in mart:
            logger.warning("Deprecated/unused config field detected: mart.sql_dir is ignored.")
        normalized["mart"] = dict(mart)

    if "bq" in normalized:
        logger.warning("Unused config field detected: bq is currently ignored by the toolkit.")

    return normalized


def _validation_error_to_value_error(exc: ValidationError, *, path: Path) -> ValueError:
    messages: list[str] = []
    for error in exc.errors():
        loc = ".".join(str(part) for part in error.get("loc", ()))
        msg = error.get("msg", "Invalid value")
        messages.append(f"{loc}: {msg}" if loc else msg)
    return _err("Config validation failed: " + "; ".join(messages), path=path)


def load_config_model(path: str | Path) -> ToolkitConfigModel:
    p = Path(path)
    base_dir = p.parent.resolve()

    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise _err(f"Impossibile leggere YAML: {e}", path=p)

    if not isinstance(data, dict):
        raise _err("dataset.yml deve essere una mappa YAML.", path=p)

    dataset_block = _require_map(data, "dataset", path=p)
    if "name" not in dataset_block:
        raise _err("Campo obbligatorio mancante o non valido: dataset.name (string).", path=p)
    if "years" not in dataset_block:
        raise _err("dataset.years deve essere una lista non vuota, es: [2022, 2023].", path=p)

    normalized = _normalize_legacy_payload(data, path=p)
    root_path, root_source = _resolve_root(normalized.get("root"), base_dir=base_dir)

    raw = normalized.get("raw", {}) or {}
    clean = normalized.get("clean", {}) or {}
    mart = normalized.get("mart", {}) or {}

    normalized_fields: list[tuple[str, Path]] = []
    if isinstance(raw, dict):
        raw, raw_changes = _normalize_section_paths("raw", raw, base_dir=base_dir)
        normalized_fields.extend(raw_changes)
    if isinstance(clean, dict):
        clean, clean_changes = _normalize_section_paths("clean", clean, base_dir=base_dir)
        normalized_fields.extend(clean_changes)
    if isinstance(mart, dict):
        mart, mart_changes = _normalize_section_paths("mart", mart, base_dir=base_dir)
        normalized_fields.extend(mart_changes)
    normalized_fields.append(("root", root_path))

    if normalized_fields:
        summary = ", ".join(f"{field}={value}" for field, value in normalized_fields)
        logger.debug("Normalized config paths: %s", summary)

    payload = {
        **normalized,
        "base_dir": base_dir,
        "root": root_path,
        "root_source": root_source,
        "raw": raw,
        "clean": clean,
        "mart": mart,
    }

    try:
        return ToolkitConfigModel.model_validate(payload)
    except ValidationError as exc:
        raise _validation_error_to_value_error(exc, path=p) from None
