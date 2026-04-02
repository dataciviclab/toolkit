from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from toolkit.core.csv_read import normalize_columns_spec


logger = logging.getLogger("toolkit.core.config")
_MANAGED_OUTPUT_ROOTS = {"_smoke_out", "_test_out"}
_SAFE_SQL_IDENTIFIER_RE = r"^[A-Za-z_][A-Za-z0-9_]*$"


@dataclass(frozen=True)
class ConfigDeprecation:
    code: str
    legacy: str
    replacement: str
    status: str
    message: str


_CONFIG_DEPRECATIONS: dict[str, ConfigDeprecation] = {
    "unknown.top_level": ConfigDeprecation(
        code="DCL009",
        legacy="unknown top-level keys",
        replacement="remove unsupported keys",
        status="ignored",
        message="unknown top-level config keys detected",
    ),
    "unknown.raw": ConfigDeprecation(
        code="DCL010",
        legacy="raw.* unknown keys",
        replacement="remove unsupported raw keys",
        status="ignored",
        message="unknown raw config keys detected",
    ),
    "unknown.clean": ConfigDeprecation(
        code="DCL011",
        legacy="clean.* unknown keys",
        replacement="remove unsupported clean keys",
        status="ignored",
        message="unknown clean config keys detected",
    ),
    "unknown.mart": ConfigDeprecation(
        code="DCL012",
        legacy="mart.* unknown keys",
        replacement="remove unsupported mart keys",
        status="ignored",
        message="unknown mart config keys detected",
    ),
    "unknown.cross_year": ConfigDeprecation(
        code="DCL013",
        legacy="cross_year.* unknown keys",
        replacement="remove unsupported cross_year keys",
        status="ignored",
        message="unknown cross_year config keys detected",
    ),
}


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


class ConfigPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strict: bool = False

    @field_validator("strict", mode="before")
    @classmethod
    def _parse_strict(cls, value: Any) -> bool:
        return parse_bool(value, "config.strict")


class ClientConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    timeout: int | None = None
    retries: int | None = None
    user_agent: str | None = None
    headers: dict[str, str] | None = None

    @field_validator("headers", mode="before")
    @classmethod
    def _validate_headers(cls, value: Any) -> dict[str, str] | None:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ValueError("raw.sources[].client.headers must be a dict")
        if not all(isinstance(k, str) and isinstance(v, str) for k, v in value.items()):
            raise ValueError("raw.sources[].client.headers must be a dict[str, str]")
        return dict(value)


class ExtractorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["identity", "unzip_all", "unzip_first", "unzip_first_csv"] = "identity"
    args: dict[str, Any] = Field(default_factory=dict)

    @field_validator("args", mode="before")
    @classmethod
    def _validate_args(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError("raw.extractor.args must be a dict")
        return dict(value)


class RawSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    type: str = "http_file"
    client: ClientConfig = Field(default_factory=ClientConfig)
    args: dict[str, Any] = Field(default_factory=dict)
    extractor: ExtractorConfig | None = None
    primary: bool = False

    @field_validator("primary", mode="before")
    @classmethod
    def _parse_primary(cls, value: Any) -> bool:
        return parse_bool(value, "raw.sources[].primary")

    @field_validator("args", mode="before")
    @classmethod
    def _validate_args(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError("raw.sources[].args must be a dict")
        return dict(value)


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
    parallel: bool | None = None
    nullstr: str | list[str] | None = None
    columns: dict[str, str] | None = None
    normalize_rows_to_columns: bool = False
    trim_whitespace: bool = True
    sample_size: int | None = None
    sheet_name: str | int | None = None
    mode: Literal["explicit", "latest", "largest", "all"] | None = None
    glob: str = "*"
    prefer_from_raw_run: bool = True
    allow_ambiguous: bool = False
    include: list[str] | None = None

    @field_validator("columns", mode="before")
    @classmethod
    def _normalize_columns(cls, value: Any) -> dict[str, str] | None:
        return normalize_columns_spec(value)

    @field_validator("normalize_rows_to_columns", mode="before")
    @classmethod
    def _normalize_rows_to_columns(cls, value: Any) -> bool:
        return parse_bool(value, "clean.read.normalize_rows_to_columns")

    @field_validator("include", mode="before")
    @classmethod
    def _normalize_include(cls, value: Any) -> list[str] | None:
        if value is None:
            return None
        return ensure_str_list(value, "clean.read.include")


class RangeRuleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min: float | None = None
    max: float | None = None


class CleanValidateConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    primary_key: list[str] = Field(default_factory=list)
    not_null: list[str] = Field(default_factory=list)
    ranges: dict[str, RangeRuleConfig] = Field(default_factory=dict)
    max_null_pct: dict[str, float] = Field(default_factory=dict)
    min_rows: int | None = None

    @field_validator("primary_key", "not_null", mode="before")
    @classmethod
    def _normalize_lists(cls, value: Any, info) -> list[str]:
        return ensure_str_list(value, f"clean.validate.{info.field_name}")


class CleanConfig(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    sql: Path | None = None
    read_mode: Literal["strict", "fallback", "robust"] = "fallback"
    read_source: Literal["auto", "config_only"] | None = None
    read: CleanReadConfig | None = None
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


class CleanValidationSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

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

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("mart.tables[].name must not be empty")
        import re

        if not re.fullmatch(_SAFE_SQL_IDENTIFIER_RE, text):
            raise ValueError(
                "mart.tables[].name must be a safe SQL identifier "
                "(letters, numbers, underscore; cannot start with a number)"
            )
        return text


class CrossYearTableConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    sql: Path
    source_layer: Literal["clean", "mart"] = "clean"
    source_table: str | None = None


class CrossYearConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tables: list[CrossYearTableConfig] = Field(default_factory=list)


class MartTableRuleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required_columns: list[str] = Field(default_factory=list)
    not_null: list[str] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    ranges: dict[str, RangeRuleConfig] = Field(default_factory=dict)
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


class MartValidationSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

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
    model_config = ConfigDict(extra="forbid")

    base_dir: Path
    schema_version: int = 1
    root: Path
    root_source: str
    dataset: DatasetBlock
    raw: RawConfig = Field(default_factory=RawConfig)
    clean: CleanConfig = Field(default_factory=CleanConfig)
    mart: MartConfig = Field(default_factory=MartConfig)
    cross_year: CrossYearConfig = Field(default_factory=CrossYearConfig)
    config: ConfigPolicy = Field(default_factory=ConfigPolicy)
    validation: GlobalValidationConfig = Field(default_factory=GlobalValidationConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)


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
    if "{year}" in text:
        sentinel = "__DCL_YEAR_PLACEHOLDER__"
        templated = text.replace("{year}", sentinel)
        path = Path(templated).expanduser()
        if path.is_absolute():
            return str(path.resolve()).replace(sentinel, "{year}")
        return str((base_dir / path).resolve()).replace(sentinel, "{year}")
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
    ),
    "mart": (
        ("tables", "*", "sql"),
    ),
    "cross_year": (
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


def _ensure_root_within_repo(root: Path, *, repo_root: Path, path: Path) -> Path:
    """
    Verify that `root` is contained within `repo_root` using resolved paths.
    Both `root` and `repo_root` must already be fully resolved by the caller.
    Returns `root` unchanged on success; raises ValueError on violation.
    `path` is the config file path, used only for error context.
    Note: this guard checks only the output root directory, not SQL input paths.
    """
    try:
        root.relative_to(repo_root)
    except ValueError as exc:
        raise _err(
            f"root resolves outside repo_root: root={root} repo_root={repo_root}",
            path=path,
        ) from exc
    return root


def _emit_deprecation_notice(
    key: str,
    *,
    strict_config: bool,
    path: Path,
) -> None:
    notice = _CONFIG_DEPRECATIONS[key]
    message = f"{notice.code} {notice.message}"
    logger.warning(message)
    if strict_config:
        raise _err(f"{notice.code} {notice.message}", path=path)


def _emit_unknown_keys_notice(
    key: str,
    extras: list[str],
    *,
    strict_config: bool,
    path: Path,
) -> None:
    notice = _CONFIG_DEPRECATIONS[key]
    formatted = ", ".join(sorted(extras))
    message = f"{notice.code} {notice.message}: {formatted}"
    logger.warning(message)
    if strict_config:
        raise _err(message, path=path)


def _declared_model_keys(model_cls: type[BaseModel]) -> set[str]:
    keys: set[str] = set()
    for field_name, field_info in model_cls.model_fields.items():
        keys.add(field_name)
        if field_info.alias:
            keys.add(str(field_info.alias))
    return keys


_TOP_LEVEL_ALLOWED_KEYS = {
    "schema_version",
    "root",
    "dataset",
    "raw",
    "clean",
    "mart",
    "cross_year",
    "config",
    "validation",
    "output",
}
_RAW_ALLOWED_KEYS = _declared_model_keys(RawConfig)
_CLEAN_ALLOWED_KEYS = _declared_model_keys(CleanConfig)
_MART_ALLOWED_KEYS = _declared_model_keys(MartConfig)
_CROSS_YEAR_ALLOWED_KEYS = _declared_model_keys(CrossYearConfig)


def _normalize_legacy_payload(
    data: dict[str, Any],
    *,
    path: Path,
    strict_config: bool,
) -> dict[str, Any]:
    normalized = dict(data)

    raw = normalized.get("raw")
    if isinstance(raw, dict):
        normalized["raw"] = dict(raw)

    clean = normalized.get("clean")
    if isinstance(clean, dict):
        normalized["clean"] = dict(clean)

    mart = normalized.get("mart")
    if isinstance(mart, dict):
        normalized["mart"] = dict(mart)

    return normalized


def _warn_or_reject_unknown_keys(
    data: dict[str, Any],
    *,
    path: Path,
    strict_config: bool,
) -> dict[str, Any]:
    normalized = dict(data)

    top_level_extras = [key for key in normalized.keys() if key not in _TOP_LEVEL_ALLOWED_KEYS]
    if "bq" in top_level_extras:
        raise _err("bq is no longer supported; remove field", path=path)
    if top_level_extras:
        _emit_unknown_keys_notice(
            "unknown.top_level",
            top_level_extras,
            strict_config=strict_config,
            path=path,
        )
        if not strict_config:
            normalized = {k: v for k, v in normalized.items() if k in _TOP_LEVEL_ALLOWED_KEYS}

    for section_name, allowed_keys, notice_key in (
        ("raw", _RAW_ALLOWED_KEYS, "unknown.raw"),
        ("clean", _CLEAN_ALLOWED_KEYS, "unknown.clean"),
        ("mart", _MART_ALLOWED_KEYS, "unknown.mart"),
        ("cross_year", _CROSS_YEAR_ALLOWED_KEYS, "unknown.cross_year"),
    ):
        section = normalized.get(section_name)
        if not isinstance(section, dict):
            continue
        extras = [key for key in section.keys() if key not in allowed_keys]
        if section_name == "raw" and "source" in extras:
            raise _err("raw.source is no longer supported; use raw.sources", path=path)
        if section_name == "clean" and "sql_path" in extras:
            raise _err("clean.sql_path is no longer supported; use clean.sql", path=path)
        if section_name == "mart" and "sql_dir" in extras:
            raise _err("mart.sql_dir is no longer supported; use mart.tables[].sql", path=path)
        if extras:
            _emit_unknown_keys_notice(
                notice_key,
                extras,
                strict_config=strict_config,
                path=path,
            )

    return normalized


def _validation_error_to_value_error(exc: ValidationError, *, path: Path) -> ValueError:
    messages: list[str] = []
    for error in exc.errors():
        loc = ".".join(str(part) for part in error.get("loc", ()))
        msg = error.get("msg", "Invalid value")
        messages.append(f"{loc}: {msg}" if loc else msg)
    return _err("Config validation failed: " + "; ".join(messages), path=path)


def _read_strict_config(data: dict[str, Any], *, path: Path) -> bool:
    raw_config = data.get("config")
    if raw_config is None:
        return False
    if not isinstance(raw_config, dict):
        raise _err("config must be a mapping.", path=path)
    strict_value = raw_config.get("strict", False)
    return parse_bool(strict_value, "config.strict")


def load_config_model(
    path: str | Path,
    *,
    strict_config: bool = False,
    repo_root: str | Path | None = None,
) -> ToolkitConfigModel:
    """
    Load and normalize toolkit config.

    repo_root is an optional guardrail for callers that need to enforce that
    the resolved effective root stays inside a known repository tree. This is
    intentionally opt-in so the toolkit can still support valid workflows that
    write outputs outside the project directory. A typical caller is external
    CI that validates dataset.yml contracts for monorepos such as
    dataset-incubator.
    """
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

    strict_mode = strict_config or _read_strict_config(data, path=p)
    normalized = _normalize_legacy_payload(data, path=p, strict_config=strict_mode)
    normalized = _warn_or_reject_unknown_keys(normalized, path=p, strict_config=strict_mode)
    root_path, root_source = _resolve_root(normalized.get("root"), base_dir=base_dir)
    if repo_root is not None:
        repo_root_path = Path(repo_root).expanduser().resolve()
        if not repo_root_path.is_dir():
            raise _err(
                f"repo_root does not exist or is not a directory: {repo_root_path}",
                path=p,
            )
        root_path = _ensure_root_within_repo(root_path, repo_root=repo_root_path, path=p)

    raw = normalized.get("raw", {}) or {}
    clean = normalized.get("clean", {}) or {}
    mart = normalized.get("mart", {}) or {}
    cross_year = normalized.get("cross_year", {}) or {}

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
    if isinstance(cross_year, dict):
        cross_year, cross_year_changes = _normalize_section_paths("cross_year", cross_year, base_dir=base_dir)
        normalized_fields.extend(cross_year_changes)
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
        "cross_year": cross_year,
    }

    try:
        return ToolkitConfigModel.model_validate(payload)
    except ValidationError as exc:
        raise _validation_error_to_value_error(exc, path=p) from None
