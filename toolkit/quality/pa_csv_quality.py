"""Qualità CSV per dataset della Pubblica Amministrazione italiana.

Valuta un CSV secondo criteri PA: encoding, naming colonne, presenza
riferimenti geografici/temporali, mappabilità a ontologie del Catalogo
Nazionale della Semantica dei Dati (schema.gov.it).

Ispirato da ``validatore-mcp`` di AgID/simba-chatbot (Piersoft per AgID).
Adattato per l'integrazione in ``toolkit preview_url`` e source-check.

Check: 4 gruppi — Struttura (12), Contenuto (9), Open Data (10), Linked Data (6).
Score: 0-100 pesato (pass=1, warn=0.5, fail=0, info=1, skip=1).
"""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass, field

# ─── Ontologia PA: mapping colonna → ontologia schema.gov.it ─────────────────

COLUMN_ONTOLOGY_MAP: dict[str, list[str]] = {
    # CLV — Geolocation
    "lat": ["CLV (Coordinate)"],
    "latitude": ["CLV (Coordinate)"],
    "lon": ["CLV (Coordinate)"],
    "lng": ["CLV (Coordinate)"],
    "longitude": ["CLV (Coordinate)"],
    "indirizzo": ["CLV (Address)"],
    "comune": ["CLV", "ISTAT"],
    "codice_istat": ["CLV", "ISTAT"],
    "codice_comune": ["CLV", "ISTAT"],
    "provincia": ["CLV"],
    "regione": ["CLV"],
    "cap": ["CLV"],
    # PC — PublicContract
    "cig": ["PC (PublicContract)"],
    "cup": ["PC (PublicContract)"],
    "importo": ["PC (PublicContract)", "QB (DataCube)"],
    "codice_ipa": ["PC (PublicContract)", "IPA"],
    # TI — TimeInterval
    "data_inizio": ["TI (TimeInterval)"],
    "data_fine": ["TI (TimeInterval)"],
    "data_aggiornamento": ["TI (TimeInterval)"],
    "anno": ["TI (TimeInterval)"],
    "anno_riferimento": ["TI (TimeInterval)"],
    # QB — DataCube
    "quantita": ["QB (DataCube)"],
    "valore": ["QB (DataCube)"],
    "totale": ["QB (DataCube)"],
    # CPV — Person
    "nome": ["CPV (Person)"],
    "cognome": ["CPV (Person)"],
    "cf": ["CPV (Person)"],
    "codice_fiscale": ["CPV (Person)"],
    # COV — Organization
    "azienda": ["COV (Organization)"],
    "ragione_sociale": ["COV (Organization)"],
    "denominazione": ["COV (Organization)"],
    "piva": ["COV (Organization)"],
    "partita_iva": ["COV (Organization)"],
    # CV — Controlled Vocabularies
    "codice_ateco": ["CV (ATECO)"],
    "ateco": ["CV (ATECO)"],
    # ISTAT demographics
    "popolazione": ["ISTAT (Demographics)"],
    "eta": ["ISTAT (Demographics)"],
    "sesso": ["ISTAT (Demographics)"],
    "maschi": ["ISTAT (Demographics)"],
    "femmine": ["ISTAT (Demographics)"],
    # STRU — School/Education
    "codice_scuola": ["STRU (Education)"],
    "codice_meccanografico": ["STRU (Education)"],
    # PARK — Parking
    "parcheggio": ["PARK (Parcheggi)"],
    "posti_auto": ["PARK (Parcheggi)"],
    # ACCO — Accommodation
    "struttura_ricettiva": ["ACCO (Accommodation)"],
    "camere": ["ACCO (Accommodation)"],
    "posti_letto": ["ACCO (Accommodation)", "SAN (Health)"],
    # POI — Points of Interest
    "farmacia": ["POI (Points of Interest)", "SAN (Health)"],
    "museo": ["POI (Points of Interest)", "CULTURAL-ON"],
}

# Nomi colonna che indicano chiavi geografiche/temporali (per check O5, O6)
GEO_KEYWORDS = frozenset(
    {
        "lat",
        "lon",
        "lng",
        "latitude",
        "longitude",
        "comune",
        "regione",
        "provincia",
        "codice_istat",
        "indirizzo",
        "cap",
        "codice_catastale",
    }
)
TIME_KEYWORDS = frozenset(
    {
        "data",
        "date",
        "anno",
        "year",
        "mese",
        "month",
        "timestamp",
        "periodo",
        "periodo_riferimento",
    }
)


@dataclass
class CheckResult:
    """Singolo check di qualità."""

    id: str
    title: str
    detail: str
    status: str  # pass | warn | fail | info | skip


@dataclass
class QualityReport:
    """Report completo qualità CSV PA."""

    score: int  # 0-100
    verdict: str  # buona_qualita | accettabile_con_riserva | non_accettabile
    critical_fail: bool = False
    summary: dict[str, int] = field(default_factory=dict)
    checks: dict[str, list[CheckResult]] = field(default_factory=dict)
    flags: list[str] = field(default_factory=list)
    ontologies: dict[str, list[str]] = field(default_factory=dict)
    separator: str | None = None
    header_count: int = 0
    row_count: int = 0


# ─── Utility ─────────────────────────────────────────────────────────────────


def _norm_header(h: str) -> str:
    """Normalizza nome colonna per matching."""
    return h.strip().lower().replace("-", "_").replace(" ", "_").replace("'", "")


def _detect_sep(raw: str) -> str:
    """Rileva separatore CSV: ``,`` ``;`` ``\\t`` ``|``."""
    line = raw.split("\n")[0] if raw else ""
    counts = {",": 0, ";": 0, "\t": 0, "|": 0}
    for ch in line:
        if ch in counts:
            counts[ch] += 1
    sep = max(counts, key=counts.get)  # type: ignore[arg-type]
    return sep


def _parse_csv(raw: str, sep: str) -> list[list[str]]:
    """Parser CSV minimale RFC 4180."""
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    rows: list[list[str]] = []
    for line in raw.split("\n"):
        if not line.strip():
            continue
        fields: list[str] = []
        cur = ""
        in_q = False
        i = 0
        while i < len(line):
            c = line[i]
            if c == '"':
                if in_q and i + 1 < len(line) and line[i + 1] == '"':
                    cur += '"'
                    i += 1
                else:
                    in_q = not in_q
            elif c == sep and not in_q:
                fields.append(cur)
                cur = ""
            else:
                cur += c
            i += 1
        fields.append(cur)
        rows.append(fields)
    return rows


# ─── CHECK: Struttura ─────────────────────────────────────────────────────────


def _checks_struttura(
    raw: str, rows: list[list[str]], sep: str, headers: list[str]
) -> list[CheckResult]:
    results: list[CheckResult] = []

    def p(id_: str, title: str, detail: str, status: str) -> None:
        results.append(CheckResult(id=id_, title=title, detail=detail, status=status))

    if not raw or not raw.strip():
        p("S1", "File vuoto", "Il CSV non contiene dati.", "fail")
        return results
    p("S1", "File non vuoto", f"{len(raw.strip())} caratteri.", "pass")

    sep_names = {
        ",": "virgola (,)",
        ";": "punto e virgola (;)",
        "\t": "tabulazione",
        "|": "pipe (|)",
    }
    p("S2", "Separatore rilevato", f"Rilevato: {sep_names.get(sep, repr(sep))}", "pass")

    if not headers or len(headers) == 0:
        p("S3", "Intestazione assente", "La prima riga non contiene intestazioni.", "fail")
        return results
    p("S3", "Intestazione presente", f"{len(headers)} colonne.", "pass")

    h_set = set(headers)
    if len(h_set) < len(headers):
        dupes = [h for h in headers if headers.count(h) > 1]
        dupes = list(dict.fromkeys(dupes))
        p("S4", "Intestazioni duplicate", f"Colonne duplicate: {', '.join(dupes)}", "fail")
    else:
        p("S4", "Intestazioni univoche", "Nessuna colonna duplicata.", "pass")

    empty_h = [h for h in headers if not h.strip()]
    if empty_h:
        p("S5", "Intestazioni vuote", f"{len(empty_h)} colonne senza nome.", "warn")
    else:
        p("S5", "Tutte le intestazioni nominate", "", "pass")

    data_rows = rows[1:]
    if not data_rows:
        p("S6", "Nessuna riga dati", "Il CSV ha solo l'intestazione.", "warn")
    else:
        irregular = [r for r in data_rows if len(r) != len(headers)]
        if irregular:
            p(
                "S6",
                "Numero colonne inconsistente",
                f"{len(irregular)} righe con campi diversi dall'intestazione ({len(headers)} attesi).",
                "fail",
            )
        else:
            p(
                "S6",
                "Numero colonne consistente",
                f"Tutte le {len(data_rows)} righe hanno {len(headers)} colonne.",
                "pass",
            )

    kb = len(raw.encode("utf-8")) / 1024
    if kb > 5120:
        p("S7", "File grande", f"{kb:.0f} KB: valutare suddivisione.", "info")
    else:
        p("S7", "Dimensione file", f"{kb:.1f} KB", "info")

    if "\ufffd" in raw:
        p(
            "S8",
            "Caratteri illeggibili (errore encoding)",
            "Trovati caratteri di sostituzione — probabile Windows-1252 letto come UTF-8.",
            "fail",
        )
    else:
        p("S8", "Nessun carattere illeggibile", "Encoding apparentemente corretto.", "pass")

    # Accentate corrotte (pattern Windows-1252 letto come UTF-8)
    _corrupt = [
        "\u00c3\u00a0",
        "\u00c3\u00a8",
        "\u00c3\u00a9",
        "\u00c3\u00b2",
        "\u00c3\u00b9",
        "\u00c3\u00ac",
    ]
    if any(p in raw for p in _corrupt):
        p(
            "S9",
            "Accentate corrotte",
            "Rilevate accentate Windows-1252 interpretate come UTF-8.",
            "fail",
        )
    else:
        p("S9", "Accentate nella norma", "", "pass")

    if raw and raw[0] == "\ufeff":
        p("S10", "BOM presente", "Preferibile UTF-8 senza BOM.", "warn")
    else:
        p("S10", "Nessun BOM", "", "pass")

    _ctrl = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
    ctrl_lines = [(i + 1, ln) for i, ln in enumerate(raw.split("\n")) if _ctrl.search(ln)]
    if ctrl_lines:
        sample = ", ".join(str(ln) for ln, _ in ctrl_lines[:3])
        p("S11", "Caratteri di controllo", f"Riga/e: {sample}.", "warn")
    else:
        p("S11", "Nessun carattere di controllo", "", "pass")

    blank_rows = [r for r in data_rows if all(c.strip() == "" for c in r)]
    if blank_rows:
        p("S12", "Righe vuote interne", f"{len(blank_rows)} righe completamente vuote.", "warn")
    else:
        p("S12", "Nessuna riga vuota interna", "", "pass")

    return results


# ─── CHECK: Contenuto ─────────────────────────────────────────────────────────


def _checks_contenuto(rows: list[list[str]], headers: list[str]) -> list[CheckResult]:
    results: list[CheckResult] = []

    def p(id_: str, title: str, detail: str, status: str) -> None:
        results.append(CheckResult(id=id_, title=title, detail=detail, status=status))

    data_rows = rows[1:]
    if not data_rows:
        p("C0", "Nessun dato", "", "skip")
        return results

    # C1 — Righe duplicate
    seen: set[str] = set()
    dupe_count = 0
    for r in data_rows:
        k = "|".join(r)
        if k in seen:
            dupe_count += 1
        seen.add(k)
    if dupe_count:
        p("C1", "Righe duplicate", f"{dupe_count} righe duplicate.", "warn")
    else:
        p("C1", "Nessuna riga duplicata", "", "pass")

    # C2 — Valori mancanti
    total_cells = len(data_rows) * len(headers)
    missing = sum(1 for r in data_rows for c in r if not c.strip())
    pct = (missing / total_cells * 100) if total_cells else 0
    if pct > 30:
        p("C2", "Molti valori mancanti", f"{pct:.1f}% celle vuote.", "fail")
    elif pct > 10:
        p("C2", "Valori mancanti", f"{pct:.1f}% celle vuote.", "warn")
    else:
        p("C2", "Valori mancanti contenuti", f"{pct:.1f}% celle vuote.", "pass")

    # C3 — Colonna ID
    norm_h = [_norm_header(h) for h in headers]
    id_candidates = [
        i
        for i, h in enumerate(norm_h)
        if h in ("id", "codice", "identifier", "uuid") or h.endswith("_id")
    ]
    if id_candidates:
        col_i = id_candidates[0]
        vals = [r[col_i].strip() for r in data_rows if col_i < len(r) and r[col_i].strip()]
        if len(set(vals)) < len(vals) * 0.98:
            p("C3", "Colonna ID con duplicati", f'"{headers[col_i]}" ha valori ripetuti.', "warn")
        else:
            p("C3", "Colonna ID univoca", f'"{headers[col_i]}" valori univoci.', "pass")
    else:
        p("C3", "Nessuna colonna ID", "Aggiungere un identificatore univoco.", "warn")

    # C4 — Tipi misti per colonna
    mixed = []
    for ci, h in enumerate(headers):
        vals = [r[ci].strip() for r in data_rows if ci < len(r) and r[ci].strip()]
        if len(vals) < 5:
            continue
        nums = sum(1 for v in vals if re.match(r"^-?\d+([.,]\d+)?$", v))
        ratio = nums / len(vals)
        if 0.5 < ratio < 0.9:
            mixed.append(f'"{h}" ({ratio * 100:.0f}% numerico)')
    if mixed:
        p("C4", "Colonne a tipo misto", "; ".join(mixed), "warn")
    else:
        p("C4", "Tipi colonna omogenei", "", "pass")

    # C5 — Date ISO
    date_cols = [
        (i, h)
        for i, h in enumerate(norm_h)
        if any(k in h for k in ("data", "date", "anno", "year", "timestamp"))
    ]
    bad_dates = []
    for ci, h in date_cols:
        vals = [r[ci].strip() for r in data_rows if ci < len(r) and r[ci].strip()]
        for v in vals[:20]:
            if not re.match(r"^\d{4}-\d{2}-\d{2}(T[\d:Z.+-]+)?$", v) and not re.match(
                r"^\d{4}$", v
            ):
                bad_dates.append(f'"{headers[ci]}" (es: {v})')
                break
    if bad_dates:
        p("C5", "Date non ISO 8601", "; ".join(bad_dates) + ". Usare YYYY-MM-DD.", "warn")
    else:
        p("C5", "Date in formato ISO 8601", "", "pass")

    # C6 — Decimali con virgola
    dec_issues = []
    for ci, h in enumerate(headers):
        vals = [r[ci].strip() for r in data_rows if ci < len(r) and r[ci].strip()]
        nums = [v for v in vals if re.match(r"^-?\d+([.,]\d+)?$", v)]
        if len(nums) > 5 and any("," in v for v in nums):
            dec_issues.append(f'"{h}"')
    if dec_issues:
        p(
            "C6",
            "Decimali con virgola",
            f"Colonne: {', '.join(dec_issues)}. Preferire il punto.",
            "warn",
        )
    else:
        p("C6", "Separatore decimale corretto", "", "pass")

    # C7 — Outlier statistici (campioni con num sufficiente)
    outliers = []
    for ci, h in enumerate(headers):
        vals = []
        for r in data_rows:
            if ci < len(r):
                try:
                    vals.append(float(r[ci].strip().replace(",", ".")))
                except (ValueError, IndexError):
                    pass
        if len(vals) < 10:
            continue
        mean = statistics.mean(vals)
        stdev = statistics.stdev(vals) if len(vals) > 1 else 0
        if stdev == 0:
            continue
        anom = [v for v in vals if abs(v - mean) > 4 * stdev]
        if anom:
            outliers.append(f'"{h}": {len(anom)} valori fuori scala (media={mean:.1f})')
    if outliers:
        p("C7", "Outlier statistici", "; ".join(outliers[:3]), "warn")
    else:
        p("C7", "Nessun outlier rilevato", "", "pass")

    # C8 — Celle molto lunghe
    long_cells = []
    for ci, h in enumerate(headers):
        max_len = max((len(r[ci]) for r in data_rows if ci < len(r)), default=0)
        if max_len > 500:
            long_cells.append(f'"{h}" (max {max_len} car.)')
    if long_cells:
        p("C8", "Celle molto lunghe", "; ".join(long_cells), "warn")
    else:
        p("C8", "Lunghezza celle nella norma", "", "pass")

    return results


# ─── CHECK: Open Data ─────────────────────────────────────────────────────────


def _checks_opendata(rows: list[list[str]], headers: list[str], raw: str = "") -> list[CheckResult]:
    results: list[CheckResult] = []

    def p(id_: str, title: str, detail: str, status: str) -> None:
        results.append(CheckResult(id=id_, title=title, detail=detail, status=status))

    data_rows = rows[1:]
    norm_h = [_norm_header(h) for h in headers]

    if len(data_rows) < 10:
        p("O1", "Dataset molto piccolo", f"{len(data_rows)} righe.", "warn")
    else:
        p("O1", "Numero righe sufficiente", f"{len(data_rows)} righe.", "pass")

    if len(headers) < 3:
        p("O2", "Poche colonne", f"{len(headers)} colonne.", "warn")
    else:
        p("O2", "Numero colonne adeguato", f"{len(headers)} colonne.", "pass")

    cryptic = [
        h for h in headers if re.match(r"^col\d+$|^campo\d+$|^field\d+$|^[a-z]$", h.strip(), re.I)
    ]
    if cryptic:
        p("O3", "Intestazioni non descrittive", f"{', '.join(cryptic)}", "warn")
    else:
        p("O3", "Intestazioni descrittive", "", "pass")

    with_spaces = [h for h in headers if " " in h.strip() or "-" in h.strip()]
    if with_spaces:
        p(
            "O4",
            "Intestazioni con spazi o trattini",
            f"{', '.join(with_spaces)}. Usare underscore.",
            "warn",
        )
    else:
        p("O4", "Intestazioni in formato ottimale", "Underscore e lowercase.", "pass")

    has_geo = any(any(k in h for k in GEO_KEYWORDS) for h in norm_h)
    if has_geo:
        p("O5", "Riferimento geografico presente", "Facilita il collegamento ai LOD.", "pass")
    else:
        p(
            "O5",
            "Nessun riferimento geografico",
            "Valutare aggiunta di coordinate o codici ISTAT.",
            "info",
        )

    has_time = any(any(k in h for k in TIME_KEYWORDS) for h in norm_h)
    if has_time:
        p("O6", "Dimensione temporale presente", "Facilita analisi temporali.", "pass")
    else:
        p("O6", "Nessuna dimensione temporale", "Valutare aggiunta colonna data.", "info")

    special_h = [h for h in headers if re.search(r"[^\w\s\-\u00C0-\u017E]", h)]
    if special_h:
        p("O7", "Caratteri speciali in intestazioni", "; ".join(special_h), "warn")
    else:
        p("O7", "Nessun carattere speciale", "", "pass")

    # URI/URL nei dati
    n_uri = sum(1 for r in data_rows for c in r if re.match(r"^https?://", c.strip()))
    if n_uri > 0:
        p("O8", "URI/URL nei dati", f"{n_uri} valori URL — buono per linked data.", "pass")
    else:
        p("O8", "Nessun URI nei dati", "Aggiungere URI migliora l'interoperabilità.", "info")

    # Booleani
    bool_cols = []
    for ci, h in enumerate(headers):
        vals = [r[ci].strip().lower() for r in data_rows[:30] if ci < len(r) and r[ci].strip()]
        bool_set = {"0", "1", "true", "false", "si", "no", "s", "n", "y", "yes", "vero", "falso"}
        if vals and all(v in bool_set for v in vals):
            bool_cols.append(h)
    if bool_cols:
        p("O9", "Colonne booleane", f"{', '.join(bool_cols)}. Usare true/false coerente.", "info")
    else:
        p("O9", "Nessuna colonna booleana", "", "pass")

    # Commenti in coda
    last_lines = raw.strip().split("\n")[-3:] if raw else []
    if any(ln.startswith("#") for ln in last_lines):
        p("O10", "Righe commento in coda", "Rimuovere per massima compatibilità.", "warn")
    else:
        p("O10", "Nessun commento in coda", "", "pass")

    return results


# ─── CHECK: Linked Data / Ontologie ────────────────────────────────────────────


def _checks_linkeddata(rows: list[list[str]], headers: list[str]) -> list[CheckResult]:
    results: list[CheckResult] = []
    data_rows = rows[1:]
    norm_h = [_norm_header(h) for h in headers]

    def p(id_: str, title: str, detail: str, status: str) -> None:
        results.append(CheckResult(id=id_, title=title, detail=detail, status=status))

    # L1 — UUID
    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
    has_uuid = False
    for ci, h in enumerate(headers):
        if any(k in h.lower() for k in ("id", "uuid", "identifier")):
            for r in data_rows[:10]:
                if ci < len(r) and uuid_re.match(r[ci].strip()):
                    has_uuid = True
                    break
        if has_uuid:
            break
    if has_uuid:
        p("L1", "UUID come identificatore", "Ottimo per URI stabili.", "pass")
    else:
        p("L1", "Nessun UUID rilevato", "Valutare uso di UUID.", "info")

    # L2 — Ontologie PA
    matched_ontos: dict[str, list[str]] = {}
    for h in headers:
        key = _norm_header(h)
        for pattern, ontos in COLUMN_ONTOLOGY_MAP.items():
            if pattern in key:
                matched_ontos[h] = ontos
                break
    if matched_ontos:
        details = [f'"{h}" → {", ".join(o)}' for h, o in matched_ontos.items()]
        p(
            "L2",
            "Colonne mappabili a ontologie italiane",
            "; ".join(details[:5]) + (f" e altre {len(details) - 5}" if len(details) > 5 else ""),
            "pass",
        )
    else:
        p("L2", "Nessuna colonna riconosciuta", "Verificare naming conventions.", "warn")

    # L3 — Codici ISTAT
    has_istat = any("istat" in h for h in norm_h)
    if has_istat:
        p("L3", "Codici ISTAT rilevati", "", "pass")
    else:
        p("L3", "Nessun codice ISTAT", "Migliora il collegamento ai LOD PA.", "info")

    # L4 — CIG / CUP
    has_cig = any("cig" in h for h in norm_h)
    has_cup = any("cup" in h for h in norm_h)
    if has_cig or has_cup:
        p("L4", "CIG/CUP rilevati", "Collegamento a PC (PublicContract).", "pass")
    else:
        p("L4", "Nessun CIG/CUP", "", "info")

    # L5 — URI ontologie note nei dati
    uri_pattern = re.compile(r"^https?://(schema\.gov\.it|w3\.org|data\.europa\.eu|dati\.gov\.it)/")
    linked_vals = 0
    for r in data_rows[:30]:
        for c in r:
            if uri_pattern.match(c.strip()):
                linked_vals += 1
    if linked_vals:
        p(
            "L5",
            "URI ontologie nei valori",
            f"{linked_vals} valori con URI schema.gov.it / w3.org.",
            "pass",
        )
    else:
        p("L5", "Nessun URI ontologico nei valori", "Considerare URI da schema.gov.it.", "info")

    # L6 — Potenziale 5 stelle
    if len(matched_ontos) >= 2 and len(headers) >= 5:
        p("L6", "Potenziale 5 stelle", "Dataset ricco e mappabile per RDF.", "pass")
    else:
        p("L6", "Dataset da arricchire", "Servono ≥5 colonne ben nominate per 5-star.", "info")

    return results, matched_ontos


# ─── Punteggio e verdetto ─────────────────────────────────────────────────────

_WEIGHTS = {"pass": 1, "warn": 0.5, "fail": 0, "info": 1, "skip": 1}

_CRITICAL_FAIL_CHECKS = frozenset({"S1", "S3", "S6"})


def _compute_score(all_checks: list[CheckResult]) -> int:
    active = [c for c in all_checks if c.status != "skip"]
    if not active:
        return 0
    total = sum(_WEIGHTS.get(c.status, 0) for c in active)
    return round(total / len(active) * 100)


def _is_critical_fail(all_checks: list[CheckResult]) -> bool:
    return any(c.id in _CRITICAL_FAIL_CHECKS and c.status == "fail" for c in all_checks)


def _build_flags(all_checks: list[CheckResult]) -> list[str]:
    fails = [c.id for c in all_checks if c.status == "fail"]
    warns = [c.id for c in all_checks if c.status == "warn"]
    flags = []
    if "S8" in fails or "S9" in fails:
        flags.append("encoding_issues")
    if "S4" in fails:
        flags.append("duplicate_columns")
    if "S6" in fails:
        flags.append("inconsistent_columns")
    if "C2" in fails:
        flags.append("high_missing_rate")
    if "O4" in warns:
        flags.append("column_naming")
    if "C5" in warns:
        flags.append("non_iso_dates")
    return flags


# ─── Entry point principale ───────────────────────────────────────────────────


def assess_quality(
    csv_text: str,
    *,
    title: str = "",
) -> QualityReport:
    """Valuta la qualità di un CSV secondo criteri PA.

    Args:
        csv_text: Testo grezzo del CSV (UTF-8).
        title: Titolo opzionale del dataset (per ontology detection
               context-aware — riservato per estensione futura).

    Returns:
        QualityReport con score, verdetto, check per categoria, flags,
        ontologie rilevate.
    """
    sep = _detect_sep(csv_text)
    rows = _parse_csv(csv_text, sep)
    headers = rows[0] if rows else []

    str_checks = _checks_struttura(csv_text, rows, sep, headers)
    con_checks = _checks_contenuto(rows, headers)
    od_checks = _checks_opendata(rows, headers, csv_text)
    ld_checks, matched_ontos = _checks_linkeddata(rows, headers)

    all_checks = str_checks + con_checks + od_checks + ld_checks
    score = _compute_score(all_checks)
    crit_fail = _is_critical_fail(all_checks)
    flags = _build_flags(all_checks)

    fail_count = sum(1 for c in all_checks if c.status == "fail")
    warn_count = sum(1 for c in all_checks if c.status == "warn")
    pass_count = sum(1 for c in all_checks if c.status == "pass")

    if crit_fail or fail_count > 3:
        verdict = "non_accettabile"
    elif fail_count > 0 or warn_count > 5:
        verdict = "accettabile_con_riserva"
    else:
        verdict = "buona_qualita"

    # Ontologie aggregate
    ontologies: dict[str, list[str]] = {}
    for col, onto_list in matched_ontos.items():
        for onto in onto_list:
            family = onto.split(" ")[0].strip("()")
            if family not in ontologies:
                ontologies[family] = []
            if onto not in ontologies[family]:
                ontologies[family].append(onto)

    return QualityReport(
        score=score,
        verdict=verdict,
        critical_fail=crit_fail,
        summary={
            "pass": pass_count,
            "warn": warn_count,
            "fail": fail_count,
            "rows": len(rows) - 1 if rows else 0,
            "columns": len(headers),
        },
        checks={
            "struttura": str_checks,
            "contenuto": con_checks,
            "opendata": od_checks,
            "linkeddata": ld_checks,
        },
        flags=flags,
        ontologies=ontologies,
        separator=sep,
        header_count=len(headers),
        row_count=len(rows) - 1 if rows else 0,
    )
