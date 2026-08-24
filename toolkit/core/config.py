"""Pipeline configuration — loaded from dataset.yml.

Replaces the previous Pydantic-based config_models (24 models, 9 files, ~1.100 righe)
with simple dataclasses in a single file (~280 righe).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

import yaml

from toolkit.core.discovery import resolve_config_path


class _DictNS(dict):
    """A dict that also supports attribute access (cfg.validation.fail_on_error)."""

    def __getattr__(self, name: str) -> object:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name: str, value: object) -> None:
        self[name] = value


def _dict2ns(d: dict) -> _DictNS:
    """Convert a nested dict to _DictNS for attribute access."""
    result = _DictNS()
    for k, v in d.items():
        if isinstance(v, dict):
            result[k] = _dict2ns(v)
        else:
            result[k] = v
    return result


# ---------------------------------------------------------------------------
# Coercion helpers
# ---------------------------------------------------------------------------


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
    raise ValueError(f"{field_name} must be a boolean-like value: true/false, 1/0, yes/no")


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


def _ensure_int_list(value: Any, field_name: str) -> list[int]:
    if value is None:
        return []
    if isinstance(value, int):
        return [value]
    if isinstance(value, list):
        return [int(v) for v in value]
    raise ValueError(f"{field_name} must be an int or a list of ints")


# ---------------------------------------------------------------------------
# Config dataclasses
# ---------------------------------------------------------------------------


@dataclass
class RangeRuleConfig:
    min: float | None = None
    max: float | None = None


@dataclass
class TransitionConfig:
    max_row_drop_pct: float | None = None
    warn_removed_columns: bool = True
    fail_on_row_drop_exceeded: bool = True

    def __post_init__(self) -> None:
        self.warn_removed_columns = parse_bool(
            self.warn_removed_columns, "transition.warn_removed_columns"
        )
        self.fail_on_row_drop_exceeded = parse_bool(
            self.fail_on_row_drop_exceeded, "transition.fail_on_row_drop_exceeded"
        )


@dataclass
class CleanValidationSpec:
    """Validation rules extracted from clean section of dataset.yml."""

    required_columns: list[str] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    not_null: list[str] = field(default_factory=list)
    ranges: dict[str, RangeRuleConfig] = field(default_factory=dict)
    max_null_pct: dict[str, float] = field(default_factory=dict)
    min_rows: int | None = None
    promotion: TransitionConfig | None = None

    @staticmethod
    def from_dict(d: dict | None) -> CleanValidationSpec | None:
        if not d:
            return None
        ranges = {}
        for k, v in (d.get("ranges") or {}).items():
            if isinstance(v, dict):
                ranges[k] = RangeRuleConfig(
                    **{kk: vv for kk, vv in v.items() if kk in ("min", "max")}
                )
            else:
                ranges[k] = v
        promote = d.get("promotion") or d.get("transition")
        return CleanValidationSpec(
            required_columns=ensure_str_list(d.get("required_columns", []), "required_columns"),
            primary_key=ensure_str_list(d.get("primary_key", []), "primary_key"),
            not_null=ensure_str_list(d.get("not_null", []), "not_null"),
            ranges=ranges,
            max_null_pct=d.get("max_null_pct", {}),
            min_rows=d.get("min_rows"),
            promotion=TransitionConfig(**promote)
            if promote and isinstance(promote, dict)
            else None,
        )


@dataclass
class MartTableRuleConfig:
    required_columns: list[str] = field(default_factory=list)
    not_null: list[str] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    ranges: dict[str, RangeRuleConfig] = field(default_factory=dict)
    max_null_pct: dict[str, float] = field(default_factory=dict)
    min_rows: int | None = None

    @staticmethod
    def from_dict(d: dict | None) -> MartTableRuleConfig | None:
        if not d:
            return None
        ranges = {}
        for k, v in (d.get("ranges") or {}).items():
            if isinstance(v, dict):
                ranges[k] = RangeRuleConfig(
                    **{kk: vv for kk, vv in v.items() if kk in ("min", "max")}
                )
            else:
                ranges[k] = v
        return MartTableRuleConfig(
            required_columns=ensure_str_list(d.get("required_columns", []), "required_columns"),
            not_null=ensure_str_list(d.get("not_null", []), "not_null"),
            primary_key=ensure_str_list(d.get("primary_key", []), "primary_key"),
            ranges=ranges,
            max_null_pct=d.get("max_null_pct", {}),
            min_rows=d.get("min_rows"),
        )


@dataclass
class MartValidationSpec:
    required_tables: list[str] = field(default_factory=list)
    table_rules: dict[str, MartTableRuleConfig] = field(default_factory=dict)
    transition: TransitionConfig = field(default_factory=TransitionConfig)

    @staticmethod
    def from_dict(d: dict | None) -> MartValidationSpec | None:
        if not d:
            return None
        rules = {}
        for k, v in (d.get("table_rules") or {}).items():
            if isinstance(v, dict):
                rules[k] = MartTableRuleConfig.from_dict(v) or MartTableRuleConfig()
            else:
                rules[k] = v
        trans = d.get("transition") or d.get("transition")
        trans_obj = (
            TransitionConfig(**trans) if trans and isinstance(trans, dict) else TransitionConfig()
        )
        return MartValidationSpec(
            required_tables=ensure_str_list(d.get("required_tables", []), "required_tables"),
            table_rules=rules,
            transition=trans_obj,
        )


@dataclass
class DuckDBConfig:
    """Configurazione motore DuckDB per il dataset (blocco ``duckdb:``).

    Es. ``duckdb.memory_limit: "4GB"`` per dataset con join pesanti
    (il default lab è 2GB via safe_connect).
    """

    memory_limit: str | None = None

    @staticmethod
    def from_dict(d: dict | None) -> DuckDBConfig | None:
        if not d:
            return None
        return DuckDBConfig(memory_limit=d.get("memory_limit"))


@dataclass
class CleanReadConfig:
    source: str = "auto"
    mode: str | None = None
    include: list[str] | None = None
    glob: str = "*"
    prefer_from_raw_run: bool = True
    allow_ambiguous: bool = False
    delim: str | None = None
    header: bool = True
    encoding: str | None = None
    decimal: str | None = None
    thousands: str | None = None
    skip: int | None = None
    auto_detect: bool | None = None
    quote: str | None = None
    escape: str | None = None
    comment: str | None = None
    ignore_errors: bool | None = None
    dateformat: str | None = None
    timestampformat: str | None = None
    strict_mode: bool | None = None
    null_padding: bool | None = None
    parallel: bool | None = None
    nullstr: str | list[str] | None = None
    columns: dict[str, str] | None = None
    normalize_rows_to_columns: bool = False
    align_by_header: bool = False
    trim_whitespace: bool = True
    sample_size: int | None = None
    sheet_name: str | int | None = None

    def __post_init__(self) -> None:
        if self.align_by_header and not self.normalize_rows_to_columns:
            raise ValueError("align_by_header=true requires normalize_rows_to_columns=true")

    @staticmethod
    def from_dict(d: dict | None) -> CleanReadConfig | None:
        if not d:
            return None
        return CleanReadConfig(**d)


@dataclass
class CleanValidateConfig:
    """Validation rules inside clean section — corresponds to clean.validate.* in YAML."""

    primary_key: list[str] = field(default_factory=list)
    not_null: list[str] = field(default_factory=list)
    ranges: dict[str, RangeRuleConfig] = field(default_factory=dict)
    max_null_pct: dict[str, float] = field(default_factory=dict)
    min_rows: int | None = None
    promotion: TransitionConfig | None = None

    @staticmethod
    def from_dict(d: dict | None) -> CleanValidateConfig | None:
        if not d:
            return None
        ranges = {}
        for k, v in (d.get("ranges") or {}).items():
            if isinstance(v, dict):
                ranges[k] = RangeRuleConfig(
                    **{kk: vv for kk, vv in v.items() if kk in ("min", "max")}
                )
            else:
                ranges[k] = v
        promote = d.get("promotion") or d.get("transition")
        return CleanValidateConfig(
            primary_key=ensure_str_list(d.get("primary_key", []), "primary_key"),
            not_null=ensure_str_list(d.get("not_null", []), "not_null"),
            ranges=ranges,
            max_null_pct=d.get("max_null_pct", {}),
            min_rows=d.get("min_rows"),
            promotion=TransitionConfig(**promote)
            if promote and isinstance(promote, dict)
            else None,
        )

    def to_dict(self) -> dict[str, Any]:
        """Replacement for old Pydantic model_dump()."""
        result: dict[str, Any] = {}
        if self.primary_key:
            result["primary_key"] = self.primary_key
        if self.not_null:
            result["not_null"] = self.not_null
        if self.ranges:
            result["ranges"] = {k: {"min": r.min, "max": r.max} for k, r in self.ranges.items()}
        if self.max_null_pct:
            result["max_null_pct"] = self.max_null_pct
        if self.min_rows is not None:
            result["min_rows"] = self.min_rows
        if self.promotion:
            result["promotion"] = asdict(self.promotion)
        return result


@dataclass
class CleanConfig:
    sql: str | Path | None = None
    read_mode: str = "fallback"
    read_source: str = "auto"
    read: CleanReadConfig | None = None
    required_columns: list[str] = field(default_factory=list)
    validate: CleanValidateConfig | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_dict(d: dict | None) -> CleanConfig:
        if not d:
            return CleanConfig()
        validate = CleanValidateConfig.from_dict(d.get("validate"))
        read = CleanReadConfig.from_dict(d.get("read"))
        known = {"sql", "read_mode", "read_source", "read", "required_columns", "validate"}
        extra = {k: v for k, v in d.items() if k not in known}
        return CleanConfig(
            sql=d.get("sql"),
            read_mode=d.get("read_mode", "fallback"),
            read_source=d.get("read_source", "auto"),
            read=read,
            required_columns=ensure_str_list(
                d.get("required_columns", []), "clean.required_columns"
            ),
            validate=validate,
            extra=extra,
        )


@dataclass
class MartTableConfig:
    name: str = ""
    sql: str | Path = ""
    years: list[int] | None = None
    source_layer: str = "clean"
    source_table: str | None = None

    @staticmethod
    def from_dict(d: dict) -> MartTableConfig:
        sql_val = d.get("sql", "")
        return MartTableConfig(
            name=str(d.get("name", "")),
            sql=Path(sql_val) if isinstance(sql_val, str) else sql_val,
            years=_ensure_int_list(d.get("years"), "mart.tables[].years") or None,
            source_layer=d.get("source_layer", "clean"),
            source_table=d.get("source_table"),
        )


@dataclass
class MartValidateConfig:
    table_rules: dict[str, MartTableRuleConfig] = field(default_factory=dict)
    transition: TransitionConfig = field(
        default_factory=lambda: TransitionConfig(warn_removed_columns=False)
    )

    @staticmethod
    def from_dict(d: dict | None) -> MartValidateConfig | None:
        if not d:
            return None
        rules = {}
        for k, v in (d.get("table_rules") or {}).items():
            if isinstance(v, dict):
                rules[k] = MartTableRuleConfig.from_dict(v) or MartTableRuleConfig()
            else:
                rules[k] = v
        trans = d.get("transition") or d.get("transition")
        trans_obj = (
            TransitionConfig(**trans)
            if trans and isinstance(trans, dict)
            else TransitionConfig(warn_removed_columns=False)
        )
        return MartValidateConfig(table_rules=rules, transition=trans_obj)

    def to_dict(self) -> dict[str, Any]:
        """Replacement for old Pydantic model_dump()."""
        from dataclasses import asdict

        result: dict[str, Any] = {}
        if self.table_rules:
            result["table_rules"] = {name: asdict(rule) for name, rule in self.table_rules.items()}
        result["transition"] = asdict(self.transition)
        return result


@dataclass
class HierarchyLevel:
    level: str = ""
    table: str = ""
    grain: list[str] = field(default_factory=list)
    source_table: str | None = None
    exclude_metrics: list[str] = field(default_factory=list)


@dataclass
class HierarchyConfig:
    axis: str = ""
    levels: list[HierarchyLevel] = field(default_factory=list)


@dataclass
class MartConfig:
    tables: list[MartTableConfig] = field(default_factory=list)
    required_tables: list[str] = field(default_factory=list)
    hierarchy: HierarchyConfig | None = None
    validate: MartValidateConfig | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_dict(d: dict | None) -> MartConfig:
        if not d:
            return MartConfig()
        tables = [
            MartTableConfig.from_dict(t) for t in (d.get("tables") or []) if isinstance(t, dict)
        ]
        req = ensure_str_list(d.get("required_tables", []), "mart.required_tables")
        if not req and tables:
            req = [t.name for t in tables]
        hierarchy_raw = d.get("hierarchy")
        hierarchy = None
        if hierarchy_raw and isinstance(hierarchy_raw, dict):
            levels = [
                HierarchyLevel(**lvl)
                for lvl in (hierarchy_raw.get("levels") or [])
                if isinstance(lvl, dict)
            ]
            hierarchy = HierarchyConfig(axis=hierarchy_raw.get("axis", ""), levels=levels)
        validate = MartValidateConfig.from_dict(d.get("validate"))
        known = {"tables", "required_tables", "hierarchy", "validate"}
        extra = {k: v for k, v in d.items() if k not in known}
        return MartConfig(
            tables=tables,
            required_tables=req,
            hierarchy=hierarchy,
            validate=validate,
            extra=extra,
        )


@dataclass
class RawSourceConfig:
    name: str | None = None
    type: str = "http_file"
    year: int | None = None
    args: dict = field(default_factory=dict)
    primary: bool = False
    inject_column: dict[str, str] | None = None
    # Client config (flattened from old client: {})
    timeout: int | None = None
    retries: int | None = None
    user_agent: str | None = None
    headers: dict[str, str] | None = None

    @staticmethod
    def from_dict(d: dict) -> RawSourceConfig:
        client = d.get("client") or {}
        return RawSourceConfig(
            name=d.get("name"),
            type=d.get("type", "http_file"),
            year=d.get("year"),
            args=d.get("args", {}),
            primary=parse_bool(d.get("primary", False), "raw.sources[].primary"),
            inject_column=d.get("inject_column"),
            timeout=client.get("timeout") if isinstance(client, dict) else None,
            retries=client.get("retries") if isinstance(client, dict) else None,
            user_agent=client.get("user_agent") if isinstance(client, dict) else None,
            headers=client.get("headers") if isinstance(client, dict) else None,
        )

    def to_dict(self) -> dict[str, Any]:
        """Replacement for old Pydantic model_dump().

        Ricostruisce il blocco ``client`` annidato (timeout/retries/user_agent/
        headers) invece di appiattirlo a livello source — così i plugin che
        leggono ``source.get("client")`` vedono il client del dataset.yml.
        """
        result: dict[str, Any] = {
            "name": self.name,
            "type": self.type,
            "args": self.args,
        }
        if self.year is not None:
            result["year"] = self.year
        if self.primary:
            result["primary"] = True
        if self.inject_column is not None:
            result["inject_column"] = self.inject_column
        client = {
            k: v
            for k, v in (
                ("timeout", self.timeout),
                ("retries", self.retries),
                ("user_agent", self.user_agent),
                ("headers", self.headers),
            )
            if v is not None
        }
        if client:
            result["client"] = client
        return result


@dataclass
class RawConfig:
    sources: list[RawSourceConfig] = field(default_factory=list)
    output_policy: str = "versioned"
    extractor: dict | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_dict(d: dict | None) -> RawConfig:
        if not d:
            return RawConfig()
        sources = [
            RawSourceConfig.from_dict(s) for s in (d.get("sources") or []) if isinstance(s, dict)
        ]
        known = {"sources", "output_policy", "extractor"}
        extra = {k: v for k, v in d.items() if k not in known}
        return RawConfig(
            sources=sources,
            output_policy=d.get("output_policy", "versioned"),
            extractor=d.get("extractor"),
            extra=extra,
        )


# ---------------------------------------------------------------------------
# PipelineConfig — the unified config object
# ---------------------------------------------------------------------------


@dataclass
class PipelineConfig:
    """Unified pipeline configuration loaded from dataset.yml."""

    root: Path = Path(".")
    base_dir: Path = Path(".")
    root_source: str = "dataset"

    # Dataset identity
    dataset: str = ""
    source_id: str | None = None
    years: list[int] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    category: str | None = None

    # Pipeline sections
    raw: RawConfig = field(default_factory=RawConfig)
    clean: CleanConfig = field(default_factory=CleanConfig)
    mart: MartConfig = field(default_factory=MartConfig)
    support: list[dict] = field(default_factory=list)

    # DuckDB engine settings (blocco ``duckdb:`` opzionale nel dataset.yml)
    duckdb: DuckDBConfig | None = field(default_factory=DuckDBConfig)

    # Global settings
    validation: dict = field(default_factory=lambda: {"fail_on_error": True, "mode": "strict"})
    output: dict = field(default_factory=lambda: {"artifacts": "standard"})

    def __post_init__(self) -> None:
        # Ensure validation and output support both dict and dot access
        if isinstance(self.validation, dict):
            validation_defaults = {"fail_on_error": True, "mode": "strict"}
            validation_defaults.update(self.validation)
            self.validation = _dict2ns(validation_defaults)
        if isinstance(self.output, dict):
            output_defaults = {"artifacts": "standard"}
            output_defaults.update(self.output)
            self.output = _dict2ns(output_defaults)

    def resolve(self, rel_path: str | Path) -> Path:
        p = Path(rel_path)
        if p.is_absolute():
            return p
        return (self.base_dir / p).resolve()

    @property
    def is_mart_only(self) -> bool:
        """``True`` se il dataset ha solo configurazione MART (no CLEAN SQL)."""
        return not bool(self.clean.sql)

    @property
    def has_multi_year_mart(self) -> bool:
        """``True`` se una tabella MART ha years espliciti (multi-year)."""
        return any(t.years for t in self.mart.tables)

    @property
    def has_single_year_mart(self) -> bool:
        """``True`` se una tabella MART non ha years (per-year) o c'è hierarchy."""
        has_single_year = any(not t.years for t in self.mart.tables)
        has_hierarchy = self.mart.hierarchy is not None
        return has_single_year or has_hierarchy


# Backward compat aliases
ToolkitConfig = PipelineConfig
ToolkitConfigModel = PipelineConfig


# ---------------------------------------------------------------------------
# ensure_dict — convert config sections to plain dicts for runner layers
# ---------------------------------------------------------------------------


def ensure_dict(cfg: Any) -> Any:
    """Convert a config section to a plain dict for runner layers.

    Handles dataclasses, old Pydantic models, dicts, and lists.

    Nota: per le dataclass i campi a ``None`` vengono esclusi
    (``{k: v ... if v is not None}``). I consumer devono usare
    ``.get()``, non ``in``/``.keys()``. Questo replica il comportamento
    del vecchio ``cli.common.dump_cfg_section`` (rimosso).
    """
    if hasattr(cfg, "to_dict"):
        return cfg.to_dict()
    if hasattr(cfg, "__dataclass_fields__"):
        return {k: v for k, v in asdict(cfg).items() if v is not None}
    if hasattr(cfg, "model_dump"):
        return cfg.model_dump(by_alias=True, exclude_none=True, exclude_unset=True)
    if isinstance(cfg, list):
        return [ensure_dict(item) for item in cfg]
    if isinstance(cfg, dict):
        return cfg
    return cfg


# ---------------------------------------------------------------------------
# Path normalization
# ---------------------------------------------------------------------------


_PATH_KEYS = {"sql", "config", "path"}


def _normalize_paths(data: dict, base_dir: Path) -> None:
    """Convert relative paths in config sections to absolute.

    Mutates data in-place. This matches the old config_models path normalization.
    """
    for section in ("raw", "clean", "mart"):
        section_data = data.get(section)
        if isinstance(section_data, dict):
            _normalize_section_paths(section_data, base_dir)
    support = data.get("support")
    if isinstance(support, list):
        for item in support:
            if isinstance(item, dict):
                if "config" in item:
                    val = item["config"]
                    if isinstance(val, str):
                        p = Path(val)
                        if not p.is_absolute():
                            item["config"] = (base_dir / p).resolve()
                # ADR-005: path del support file normalizzato sul root candidate
                if "path" in item:
                    val = item["path"]
                    if isinstance(val, str):
                        p = Path(val)
                        if not p.is_absolute():
                            item["path"] = str((base_dir / p).resolve())


def _normalize_section_paths(section: dict, base_dir: Path) -> None:
    """Normalize paths in a section dict (raw, clean, or mart).

    Handles nested structures: plain values, lists of dicts, nested dicts.
    Normalized paths are stored as Path objects (matching old Pydantic behavior).
    """
    for key, value in list(section.items()):
        if isinstance(value, str) and key in _PATH_KEYS:
            p = Path(value)
            if not p.is_absolute():
                section[key] = (base_dir / p).resolve()
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _normalize_section_paths(item, base_dir)
        elif isinstance(value, dict):
            _normalize_section_paths(value, base_dir)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def load_config(
    path: str | Path | None = None,
    *,
    strict_config: bool = False,
    repo_root: str | Path | None = None,
    root_override: str | Path | None = None,
) -> PipelineConfig:
    """Load and normalize toolkit config from dataset.yml.

    Returns a PipelineConfig dataclass with all fields populated.

    Args:
        path: Path to dataset.yml. Può essere:
            - Un path esplicito (``-c dataset.yml``)
            - Uno slug risolto automaticamente
            - ``None``: auto-detect da CWD o risalita directory
        strict_config: If True, warns on unknown keys
        repo_root: Optional guardrail to enforce root stays within repo
        root_override: Optional override for output root
    """
    resolved = resolve_config_path(path)
    p = Path(resolved)
    base_dir = p.parent.resolve()

    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise ValueError(f"Cannot read YAML: {e}") from e

    if not isinstance(data, dict):
        raise ValueError("dataset.yml must be a YAML mapping.")

    # Unknown keys warning (if strict)
    if strict_config:
        _check_unknown_keys(data, strict=strict_config, path=p)

    # Root resolution — root_source labels match old test expectations
    root_source: str
    root_raw = data.get("root")
    if root_raw:
        root = Path(str(root_raw))
        if not root.is_absolute():
            root = (base_dir / root).resolve()
        root_source = "yml"
    else:
        env_root = os.environ.get("DCL_ROOT")
        tool_outdir = os.environ.get("TOOLKIT_OUTDIR")
        if env_root:
            root = Path(env_root).resolve()
            root_source = "env:DCL_ROOT"
        elif tool_outdir:
            root = Path(tool_outdir).resolve()
            root_source = "env:TOOLKIT_OUTDIR"
        else:
            root = base_dir
            root_source = "base_dir_fallback"

    if root_override:
        root = Path(root_override).expanduser().resolve()
        root_source = "--root"

    # Repo root guardrail
    if repo_root is not None:
        repo_root_path = Path(repo_root).expanduser().resolve()
        if not repo_root_path.is_dir():
            raise ValueError(f"repo_root does not exist or is not a directory: {repo_root_path}")
        try:
            root.relative_to(repo_root_path)
        except ValueError:
            raise ValueError(
                f"Resolved root {root} is not within repo_root {repo_root_path}"
            ) from None

    # Dataset block
    dataset_block = data.get("dataset", {})
    if not isinstance(dataset_block, dict):
        raise ValueError("dataset must be a mapping.")

    # Check only for missing keys (empty values like '' or [] are validated later)
    if "name" not in dataset_block:
        raise ValueError("Required field missing or invalid: dataset.name (string).")
    name = dataset_block["name"]

    if "years" not in dataset_block:
        raise ValueError("dataset.years must be a non-empty list, e.g. [2022, 2023].")
    years_raw = dataset_block["years"]
    try:
        years = [int(y) for y in years_raw]
    except (TypeError, ValueError):
        raise ValueError("dataset.years must contain integers.")

    # Path normalization: convert relative paths in raw/clean/mart/support to absolute
    _normalize_paths(data, base_dir)

    # Support validation
    support = data.get("support", [])
    if isinstance(support, list):
        support_names: list[str] = [
            str(s["name"])
            for s in support
            if isinstance(s, dict) and isinstance(s.get("name"), str)
        ]
        duplicates = sorted({n for n in support_names if support_names.count(n) > 1})
        if duplicates:
            raise ValueError("support[].name values must be unique: " + ", ".join(duplicates))

        # ADR-005: validazione del tipo e dei campi richiesti per tipo
        from toolkit.core.support import SUPPORT_TYPES

        for s in support:
            if not isinstance(s, dict):
                continue
            stype = str(s.get("type") or "dataset")
            if stype not in SUPPORT_TYPES:
                raise ValueError(
                    f"support[].type must be one of {SUPPORT_TYPES}, got {stype!r} "
                    f"for support '{s.get('name')}'"
                )
            if stype == "dataset" and "config" not in s:
                raise ValueError(f"support dataset '{s.get('name')}' requires 'config'")
            if stype == "codelist" and not s.get("id"):
                raise ValueError(f"support codelist '{s.get('name')}' requires 'id'")
            if stype == "file" and not s.get("path"):
                raise ValueError(f"support file '{s.get('name')}' requires 'path'")

    # Convert support entries to dict-like objects with attribute access
    support_objects = [_dict2ns(s) if isinstance(s, dict) else s for s in support]

    return PipelineConfig(
        root=root,
        base_dir=base_dir,
        root_source=root_source,
        dataset=name,
        source_id=dataset_block.get("source_id"),
        years=years,
        tags=ensure_str_list(dataset_block.get("tags", []), "dataset.tags"),
        category=dataset_block.get("category"),
        raw=RawConfig.from_dict(data.get("raw")),
        clean=CleanConfig.from_dict(data.get("clean")),
        mart=MartConfig.from_dict(data.get("mart")),
        support=support_objects,
        duckdb=DuckDBConfig.from_dict(data.get("duckdb")),
        validation=data.get("validation", {"fail_on_error": True, "mode": "strict"}),
        output=data.get("output", {"artifacts": "standard"}),
    )


def _check_unknown_keys(data: dict, *, strict: bool, path: Path) -> None:
    """Basic unknown key check for strict mode."""
    allowed = {
        "root",
        "schema_version",
        "dataset",
        "raw",
        "clean",
        "mart",
        "support",
        "config",
        "validation",
        "output",
    }
    unknown = set(data.keys()) - allowed
    if unknown:
        msg = f"Unknown top-level config keys: {', '.join(sorted(unknown))}"
        if strict:
            raise ValueError(msg)
        import logging

        logging.getLogger("toolkit.core.config").warning("%s in %s", msg, path)
