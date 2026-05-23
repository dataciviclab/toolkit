# Prompt: fix refactor scout (toolkit + SO)

Applica queste correzioni sugli stessi branch (`refactor/init-url-enriched` su toolkit, `refactor/scout-integration` su SO). Non mergiare nulla — resta su entrambi i branch.

---

## 1. TOOLKIT: contratti allineati prima di toccare SO

### 1a. `resolve_preview_kind` deve restituire UPPERCASE

**File**: `toolkit/scout/http.py` — funzione `resolve_preview_kind`
**Problema**: restituisce lowercase (`"csv"`, `"json"`, etc.), ma SO e tutto il resto dello stack usano uppercase (`"CSV"`, `"JSON"`).
**Fix**: Cambia i return value in uppercase. Cerca ogni `return "..."` nella funzione e rendi il valore UPPERCASE.

```python
# Prima:
return "csv"
return "json"

# Dopo:
return "CSV"
return "JSON"
```

**Verifica**: cerca tutti i `return "..."` dentro `resolve_preview_kind` e converti in uppercase. Poi cerca chiamate a `resolve_preview_kind` in tutto il branch e verifica che chi la chiama non faccia `.lower()` o `.upper()` in seguito (se lo fa, rimuovi l'adattamento). `_PREVIEW_KINDS` set deve restare lowercase (è un set interno di lookup).

### 1b. `infer_years` — aggiungi `_YEAR_START_RE` mancante

**File**: `toolkit/scout/infer.py`
**Problema**: SO aveva 3 regex per anni, toolkit ne ha 2 (manca `_YEAR_START_RE`).
**Fix**: Aggiungi la regex e il loop.

```python
# Dopo le regex esistenti:
_YEAR_START_RE = re.compile(r"(?:^|(?<!\d))(20[012]\d)")

# Dentro infer_years(), dopo il compact loop:
for y in _YEAR_START_RE.findall(text):
    years.add(int(y))
```

**Verifica**: `infer_years("2023 testo")` deve restituire `(2023, 2023)`. La regex cattura anni anche all'inizio della stringa o dopo boundary non-digit.

### 1c. Sposta SQL generation fuori da `infer.py`

**File**: `toolkit/scout/infer.py` → `toolkit/scout/scaffold.py`
**Problema**: `suggest_clean_sql()`, `suggest_mart_sql()`, `suggest_validation()` stanno in `infer.py` ma sono logica di scaffold, non inferenza.
**Fix**: Sposta queste tre funzioni in `scaffold.py` (dove già c'è logica SQL). Aggiorna gli import in `__init__.py` e in `cmd_scout.py` (che importa `suggest_clean_sql`/`suggest_mart_sql` via `from toolkit.scout.infer import ...`).

**Attenzione**: `cmd_scout.py` usa `from toolkit.scout.infer import suggest_validation` — va cambiato in `from toolkit.scout.scaffold import suggest_validation`. `scaffold.py` già importa `from toolkit.scout.infer import suggest_clean_sql, suggest_mart_sql` — inverti la dipendenza.

### 1d. Rimuovi `generate_yaml_scaffold()` (codice morto)

**File**: `toolkit/scout/scaffold.py`
**Problema**: `generate_yaml_scaffold()` è marcato "conservata per test, non usata dalla CLI" ma non è testata.
**Fix**: Elimina la funzione e il suo helper `_generate_raw_sources_block()` (quello legacy, non i 4 nuovi `_generate_raw_sources_block_*`). Verifica che nessun test la referenzi.

### 1e. Assottiglia `cmd_scout.py`

**File**: `toolkit/cli/cmd_scout.py`
**Problema**: 503 righe — troppa logica di orchestrazione. `_scaffold_sdmx()` duplica `generate_full_scaffold()`.
**Fix**: 
1. Estrai `_scaffold_file()`, `_scaffold_html()`, `_scaffold_ckan()`, `_scaffold_sdmx()` in un nuovo modulo `toolkit/scout/orchestrate.py`. Il CLI chiama una singola funzione `orchestrate_scaffold(url, probe_result, ...)`.
2. Modifica `generate_full_scaffold()` in `scaffold.py` per gestire nativamente SDMX (parametro `source_type`), così `_scaffold_sdmx()` non duplica.

Dopo: `cmd_scout.py` deve essere < 200 righe (solo parsing argomenti, chiamate a libreria, output).

---

## 2. SO: rimuovi spazzatura e testa l'integrazione

### 2a. Rimuovi i 24 file `.cover` dalla storia

**Branch**: SO, `refactor/scout-integration`
**Problema**: 24 file `.cover` committati nel primo commit (`108119d`), mai rimossi.
**Fix**: Usa `git rm` per eliminare tutti i file `.cover` dalla history del branch:

```bash
# Lista completa dei file cover committati:
git ls-tree -r refactor/scout-integration --name-only | grep ',cover'

# Rimuovili (nuovo commit pulito all'ultimo):
git rm '*.py,cover'
git commit -m "chore: rimuove file .cover committati per errore"
```

**Verifica**: `git ls-tree -r HEAD --name-only | grep ',cover'` deve restituire 0 risultati.

### 2b. `_fetch_ckan_package`: togli `except Exception` nudo

**File**: `scripts/source_check_fetch.py` (branch SO)
**Problema**: `try: return _toolkit_ckan_package(...) except Exception: return None` silenzia errori.
**Fix**:

```python
def _fetch_ckan_package(base_api: str, item_name: str) -> Optional[dict]:
    parsed = urllib.parse.urlparse(base_api)
    portal_url = f"{parsed.scheme}://{parsed.netloc}"
    client = _get_circuit_client()
    try:
        pkg = _toolkit_ckan_package(portal_url, item_name, client=client)
        if pkg is None:
            logger.warning("CKAN package_show returned None for %s (portal: %s)", item_name, portal_url)
        return pkg
    except Exception as exc:
        logger.error("CKAN package_show failed for %s (portal: %s): %s", item_name, portal_url, exc)
        return None
```

### 2c. `_http_head_with_retry`: rimuovi adattamento uppercase

**File**: `scripts/source_check_fetch.py`
**Problema**: dopo il fix 1a, toolkit già restituisce uppercase. L'adattamento diventa ridondante.
**Fix**: Cambia da:

```python
fmt = _toolkit_preview_kind(url, ct, cd)
fmt_upper = fmt.upper() if fmt else None
```

a:

```python
fmt = _toolkit_preview_kind(url, ct, cd)
```

### 2d. Aggiungi test di integrazione SO↔toolkit

**File**: nuovo `tests/test_integration_toolkit_scout.py`
**Problema**: Zero test nuovi in SO. L'integrazione con toolkit non è testata.
**Fix**: Crea 3 test minimali:

```python
"""Test integrazione SO ↔ toolkit.scout."""

from toolkit.scout.http import resolve_preview_kind
from toolkit.scout.infer import infer_years, infer_granularity
from scripts.source_check_fetch import _http_head_with_retry  # solo se testabile senza rete
from scripts.source_check_analyze import _parse_ckan_package


def test_preview_kind_uppercase():
    """toolkit.resolve_preview_kind deve restituire UPPERCASE."""
    # CSV via Content-Type
    assert resolve_preview_kind("http://example.com/data", content_type="text/csv") == "CSV"
    # JSON via Content-Type
    assert resolve_preview_kind("http://example.com/data", content_type="application/json") == "JSON"
    # XLSX via URL extension
    assert resolve_preview_kind("http://example.com/data.xlsx") == "XLSX"


def test_infer_years_start_regex():
    """infer_years deve catturare anni all'inizio stringa."""
    ymin, ymax = infer_years("2023 report annuale")
    assert ymin == 2023
    assert ymax == 2023


def test_infer_years_compact():
    """infer_years deve catturare anni compatti."""
    ymin, ymax = infer_years("202122")
    assert ymin == 2021
    assert ymax == 2022
```

**Nota**: i test usano solo funzioni pure (nessuna rete). Devono passare con `pytest tests/test_integration_toolkit_scout.py`.

---

## 3. Cross-repo: verifica finale

Dopo tutti i fix, verifica:

```bash
# Toolkit: test passano
cd toolkit && git checkout refactor/init-url-enriched
pytest tests/ -x -q --timeout=30 2>&1 | tail -5
# Devono passare tutti

# SO: test passano
cd source-observatory && git checkout refactor/scout-integration
pytest tests/ -x -q --timeout=30 2>&1 | tail -5
# Devono passare tutti

# Nessun file .cover residuo
git ls-tree -r refactor/scout-integration --name-only | grep ',cover' | wc -l
# Deve restituire 0

# Contratto formato: toolkit restituisce uppercase
cd toolkit && python -c "
from toolkit.scout.http import resolve_preview_kind
r = resolve_preview_kind('http://x.com/d.csv')
assert r == 'CSV', f'Expected CSV, got {r}'
print(f'resolve_preview_kind OK: {r}')
"

# Nessun adattamento case in SO
cd source-observatory && grep -n 'fmt.upper\|fmt_lower\|\.upper()' scripts/source_check_fetch.py
# Deve restituire 0 matches (nessun workaround case)
```
