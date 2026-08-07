# Toolkit Conventions

## 1. Paths & Metadata
- **Risoluzione Path**: I path in `dataset.yml` sono relativi alla directory del file YAML.
- **RAW Metadata**: Ogni run scrive `raw/<dataset>/<year>/metadata.json` con `primary_output_file` (usato da CLEAN).
- **Audit**: `metadata.json` e `validation.json` accompagnano ogni layer con versioni di schema e audit trail.

## 2. Artifacts Policy
`output.artifacts` non ha più effetto — profiling e SQL renderizzati sono sempre generati.
Il campo è accettato per backward compatibilità ma ignorato.

`output.legacy_aliases` è stato rimosso — non ha più effetto. Il profiler scrive solo `raw_profile.json`.

## 3. CLEAN Input & Reader Logic
Il layer CLEAN segue questa precedenza di configurazione:
1. **Metadata-first**: Se `metadata.json` è valido, usa `primary_output_file`.
2. **Reader Config**: `defaults -> suggested (formatted _profile) -> config_overrides`.
3. **Read Mode**: `strict`, `fallback` (default), `robust` (forza tipi non-breaking).

La forma canonica è `clean.read.source: auto|config_only`. Gli alias legacy
(`clean.read: auto`, `clean.read.csv.*`) restano compatibilità, non modello per nuove config.

## 4. Positional Fixed Schema
Usa `normalize_rows_to_columns: true` per CSV multi-anno instabili con schema posizionale:
- Richiede `clean.read.columns` (mapping fisso).
- **Senza `align_by_header`** (default): padding a destra per righe corte, fallisce su colonne in eccesso.
  Ideale per fonti IRPEF, AIFA, SIOPE con drift di colonne finali.
- **Con `align_by_header: true`**: allineamento per nome colonna invece che posizionale.
  Colonne attese ma non nell'header → stringa vuota. Colonne CSV extra → ignorate.
  Colonne in ordine diverso → riallineate. Ideale per fonti BDAP dove colonne
  intermedie appaiono/scompaiono tra anni (`header: true` obbligatorio).

## 5. Quirks fonti PA Italiane (Checklist)

| Quirks | Soluzione / Prevenzione |
| --- | --- |
| **Encoding** | Prevale `cp1252` (Windows). Esplicita `encoding: cp1252`. |
| **Header non standard** | Usa `header: false` e `skip: N` per saltare righe spurie. |
| **Schema Drift** | Usa `toolkit inspect config --diff` per rilevare cambi tra anni. |
| **Chiavi Geografiche** | Non normalizzare nel `clean.sql` senza documentare in `notes.md`. |
| **ZIP/XLSX** | Il toolkit non estrae ZIP; usa l'extractor corretto. |

## 6. Standard SQL Macros

Il toolkit carica automaticamente **8 macro DuckDB** in ogni esecuzione del layer CLEAN.
Sostituiscono i pattern boilerplate (`TRY_CAST`, `REPLACE` per numeri italiani,
`CASE` per boolean, `TRIM`) con chiamate leggibili.

### Macro disponibili

| Macro | Sostituisce | Frequenza in DI |
|---|---|---|
| `normalize_string(col)` | `TRIM(CAST(... AS VARCHAR))` + `''` → `NULL` | 100% |
| `cast_int(col)` | `TRY_CAST(... AS INTEGER)` | 100% |
| `cast_bigint(col)` | `TRY_CAST(... AS BIGINT)` | 100% |
| `cast_double(col)` | `TRY_CAST(... AS DOUBLE)` | 100% |
| `normalize_italian_number(col)` | `REPLACE(REPLACE(... , '.', ''), ',', '.')` | 40% |
| `normalize_italian_integer(col)` | Come sopra + `CAST(... AS INTEGER)` | 30% |
| `decode_flag(col, 'X')` | `CASE WHEN TRIM(col)='X' THEN TRUE ELSE FALSE END` | 30% |
| `remove_dot_thousands(col)` | `REPLACE(... , '.', '')` su interi | 20% |

### Regole

- **Stile raccomandato**: nei nuovi `clean.sql` usare le macro. Lo scaffold (`toolkit run init`) genera già SQL con macro.
- **Retrocompatibile**: i `clean.sql` esistenti continuano a funzionare senza modifiche. Le macro sono additive.
- **Nessuna config**: non serve abilitarle in `dataset.yml` — sono sempre caricate.
- **Testabili**: `pytest tests/test_macros_sql.py -v` (38 test).

Dettaglio completo: [standard-macros.md](standard-macros.md).

## 7. Validation Gate
- RAW: `raw_validation.json`
- CLEAN: `_validate/clean_validation.json`
- MART: `_validate/mart_validation.json`
- Config: `validation.fail_on_error: true` ferma la pipeline al primo blocco.

Versioni schema stabili:
- `metadata.json` include `metadata_schema_version`.
- Ogni validation report include `validation_schema_version`.
- I metadata CLEAN espongono `read_params_source`, `read_source_used`, `read_params_used`.

## 8. Support dataset (ADR-005)
- I support (dataset/codelist/file) si dichiarano in `support:` nel dataset.yml e si
  referenziano nel SQL **solo** con i placeholder `{support.NAME.*}` — mai con path
  hardcoded (il drift è segnalato come warning in dry-run da `check_support_path_drift`).
- Placeholder disponibili: `{support.NAME.mart}` (prima tabella), `{support.NAME.mart.TABLE}`,
  `{support.NAME.clean}`, `{support.NAME.path}` (codelist/file), `{support.NAME.outputs}`.
- Orchestrazione: i support sono eseguiti prima del candidate e riusati se gli output
  (clean+mart per dataset; parquet canonico per codelist; path per file) sono già presenti.
  Rigenerazione forzata: `toolkit run --refresh-support`.
- Codelist SDMX: parquet canonico in `out/data/support/{name}/{name}.parquet`, fuori dal
  catalogo clean (le codelist non sono dataset).
- Riferimento: [ADR-005](adr/005-support-ensure.md), schema in [config-schema.md](config-schema.md).

Rimandi:
- schema completo: [config-schema.md](config-schema.md)
- workflow avanzati e legacy boundary: [advanced-workflows.md](advanced-workflows.md)
- contratto notebook/output: [notebook-contract.md](notebook-contract.md)

---
**Done**: Contratti tecnici fissati, quirks operativi mappati.
