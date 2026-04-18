# Toolkit Conventions

## 1. Paths & Manifests
- **Risoluzione Path**: I path in `dataset.yml` sono relativi alla directory del file YAML.
- **RAW Manifest**: Ogni run scrive `raw/<dataset>/<year>/manifest.json` con `primary_output_file` (usato da CLEAN).
- **Audit**: `metadata.json` e `validation.json` accompagnano ogni layer con versioni di schema e audit trail.

## 2. Artifacts Policy
Il toolkit supporta tre livelli di conservazione artefatti (`output.artifacts`):
- `minimal`: Solo file primari, parquet finale, manifest e metadata essenziali.
- `standard` (**default**): Aggiunge `raw_profile.json` e SQL generati in `_run/*.sql`.
- `debug`: Tutti gli output intermedi, report e alias di compatibilità legacy.

`output.legacy_aliases: true` conserva alias di compatibilità dove supportati; nei nuovi repo usare `--strict-config` per intercettare campi legacy.

## 3. CLEAN Input & Reader Logic
Il layer CLEAN segue questa precedenza di configurazione:
1. **Manifest-first**: Se `manifest.json` è valido, usa `primary_output_file`.
2. **Reader Config**: `defaults -> suggested (formatted _profile) -> config_overrides`.
3. **Read Mode**: `strict`, `fallback` (default), `robust` (forza tipi non-breaking).

La forma canonica è `clean.read.source: auto|config_only`. Gli alias legacy
(`clean.read: auto`, `clean.read.csv.*`) restano compatibilità, non modello per nuove config.

## 4. Positional Fixed Schema
Usa `normalize_rows_to_columns: true` per CSV multi-anno instabili con schema posizionale:
- Richiede `clean.read.columns` (mapping fisso).
- Pad-da a destra le righe corte, fallisce su quelle col numero di colonne in eccesso.
- Ideale per fonti IRPEF, AIFA, SIOPE con drift di colonne finali.

## 5. Quirks fonti PA Italiane (Checklist)

| Quirks | Soluzione / Prevenzione |
| --- | --- |
| **Encoding** | Prevale `cp1252` (Windows). Esplicita `encoding: cp1252`. |
| **Header non standard** | Usa `header: false` e `skip: N` per saltare righe spurie. |
| **Schema Drift** | Usa `toolkit inspect schema-diff` per rilevare cambi tra anni. |
| **Chiavi Geografiche** | Non normalizzare nel `clean.sql` senza documentare in `notes.md`. |
| **ZIP/XLSX** | Il toolkit non estrae ZIP; usa l'extractor corretto. |

## 6. Validation Gate
- RAW: `raw_validation.json`
- CLEAN: `_validate/clean_validation.json`
- MART: `_validate/mart_validation.json`
- Config: `validation.fail_on_error: true` ferma la pipeline al primo blocco.

Versioni schema stabili:
- `metadata.json` include `metadata_schema_version`.
- Ogni validation report include `validation_schema_version`.
- I metadata CLEAN espongono `read_params_source`, `read_source_used`, `read_params_used`.

Rimandi:
- schema completo: [config-schema.md](config-schema.md)
- workflow avanzati e legacy boundary: [advanced-workflows.md](advanced-workflows.md)
- contratto notebook/output: [notebook-contract.md](notebook-contract.md)

---
**Done**: Contratti tecnici fissati, quirks operativi mappati.
