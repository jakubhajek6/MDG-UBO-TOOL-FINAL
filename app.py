
import os
import re
import base64
import sqlite3
from io import BytesIO
from pathlib import Path
from datetime import datetime
import unicodedata

import streamlit as st
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

from importer.ares_vr_client import AresVrClient
from importer.ownership_resolve_online import resolve_tree_online
from importer.graphviz_render import build_graphviz_from_nodelines_bfs

# ===== PATH pro 'dot' (Graphviz) – doplnění běžných cest =====
for p in ("/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/opt/local/bin", "/snap/bin"):
    if p not in os.environ.get("PATH", ""):
        os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + p

# ===== STREAMLIT PAGE CONFIG =====
st.set_page_config(page_title="MDG UBO Tool", layout="wide")

# ===== THEME / CSS =====
PRIMARY = "#2EA39C"
CSS = f"""
<style>
/* Buttons */
.stButton > button, .stDownloadButton > button {{
  background-color: {PRIMARY} !important;
  color: white !important;
  border: 1px solid {PRIMARY} !important;
}}
/* Progress */
div.stProgress > div > div {{
  background-color: {PRIMARY} !important;
}}
/* Links */
a, a:visited {{ color: {PRIMARY}; }}

/* Slider */
.stSlider div[data-baseweb="slider"] [class*="rail"] {{ background-color: #e6e6e6 !important; }}
.stSlider div[data-baseweb="slider"] [class*="track"] {{ background-color: {PRIMARY} !important; }}
.stSlider div[data-baseweb="slider"] [class*="thumb"] {{ background-color: {PRIMARY} !important; border: 2px solid {PRIMARY} !important; }}

/* Header inline */
.header-row {{
  display: flex; align-items: center; gap: 8px; margin: 0; padding: 0;
}}
.header-row img.logo {{ height: 140px; width: auto; display: inline-block; }}
.header-row h2 {{ margin: 0; padding: 0; line-height: 1.1; }}
.header-caption {{ margin-top: 4px; }}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# ===== Logo z kořene projektu =====
def load_project_logo() -> tuple[bytes | None, str]:
    candidates = ("logo.png", "logo.jpg", "logo.jpeg")
    for fname in candidates:
        p = Path(fname)
        if p.exists():
            data = p.read_bytes()
            ext = p.suffix.lower()
            if ext == ".png":
                return data, "image/png"
            if ext in (".jpg", ".jpeg"):
                return data, "image/jpeg"
            return data, "image/png"
    return None, ""

def img_bytes_to_data_uri(data: bytes | None, mime: str) -> str:
    if not data or not mime:
        return ""
    import base64 as _b64
    return f"data:{mime};base64,{_b64.b64encode(data).decode('ascii')}"

logo_bytes, logo_mime = load_project_logo()
data_uri = img_bytes_to_data_uri(logo_bytes, logo_mime)

# ===== PDF FONT s diakritikou =====
FONT_PATH = Path("assets") / "DejaVuSans.ttf"
PDF_FONT_NAME = "DejaVuSans"
if FONT_PATH.exists():
    try:
        pdfmetrics.registerFont(TTFont(PDF_FONT_NAME, str(FONT_PATH)))
    except Exception:
        PDF_FONT_NAME = "Helvetica"
else:
    PDF_FONT_NAME = "Helvetica"

# ===== Helpers =====
def progress_ui():
    bar = st.progress(0)
    msg = st.empty()
    def cb(text: str, p: float):
        msg.write(text)
        bar.progress(max(0, min(100, int(p * 100))))
    return cb

# Odsazení v renderovaném textu -> hloubka
INDENT_RE = re.compile(r"^( +)(.*)$")

def _line_depth_text(ln):
    if hasattr(ln, "text"):
        return int(getattr(ln, "depth", 0) or 0), str(getattr(ln, "text", ""))
    if isinstance(ln, dict):
        return int(ln.get("depth", 0) or 0), str(ln.get("text", ""))
    if isinstance(ln, (tuple, list)) and len(ln) >= 2:
        return int(ln[0] or 0), str(ln[1])
    if isinstance(ln, str):
        s = ln.rstrip("\n")
        m = INDENT_RE.match(s)
        if m:
            spaces = len(m.group(1))
            depth = spaces // 4
            return depth, m.group(2).strip()
        return 0, s
    return 0, str(ln)

def _ensure_list(x):
    if x is None: return []
    if isinstance(x, (list, tuple)): return list(x)
    return [x]

def _normalize_resolve_result(res):
    if isinstance(res, tuple):
        lines = res[0] if len(res) >= 1 else []
        warnings = res[1] if len(res) >= 2 else []
        return _ensure_list(lines), _ensure_list(warnings)
    return _ensure_list(res), []

def render_lines(lines):
    items = _ensure_list(lines)
    out = []
    for ln in items:
        depth, text = _line_depth_text(ln)
        indent = "    " * max(0, depth)
        out.append(f"{indent}{text}")
    return out

RE_COMPANY_HEADER = re.compile(r"^(?P<name>.+)\s+\(IČO\s+(?P<ico>\d{7,8})\)\s*$")
ICO_IN_LINE = re.compile(r"\(IČO\s+(?P<ico>\d{7,8})\)")
DASH_SPLIT = re.compile(r"\s+[—–-]\s+")

def extract_companies_from_lines(lines) -> list[tuple[str, str]]:
    items = _ensure_list(lines)
    found: dict[str, str] = {}
    for ln in items:
        _, t = _line_depth_text(ln)
        tt = (t or "").strip()
        if not tt:
            continue
        hm = RE_COMPANY_HEADER.match(tt)
        if hm:
            found[hm.group("ico").zfill(8)] = hm.group("name").strip()
            continue
        im = ICO_IN_LINE.search(tt)
        if im:
            ico = im.group("ico").zfill(8)
            left = tt[:im.start()].strip()
            parts = DASH_SPLIT.split(left, maxsplit=1)
            name = (parts[0] if parts else left).strip()
            found[ico] = name
    return sorted([(name, ico) for ico, name in found.items()], key=lambda x: x[0].lower())

# ===== DB inicializace + migrace schématu =====
def ensure_ares_cache_db(db_path: str, schema_path: str | None = "db/schema.sql"):
    """
    Garantuje schéma SQLite a migruje staré sloupce cache na nové.
    - Aplikuje schema.sql (CREATE TABLE IF NOT EXISTS ...)
    - Migruje ares_vr_cache: payload -> payload_json; updated_at -> fetched_at
    """
    if not db_path:
        return
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        c = conn.cursor()

        # Aplikace schema.sql (idempotentně)
        if schema_path and Path(schema_path).exists():
            sql = Path(schema_path).read_text(encoding="utf-8")
            c.executescript(sql)
            conn.commit()
        else:
            # Fallback: jen cache tabulka
            c.execute("""
                CREATE TABLE IF NOT EXISTS ares_vr_cache (
                    ico TEXT PRIMARY KEY,
                    fetched_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_ares_vr_cache_fetched_at ON ares_vr_cache(fetched_at)")
            conn.commit()

        # Migrace starého schématu -> nové názvy
        cols = {row[1] for row in c.execute("PRAGMA table_info(ares_vr_cache)").fetchall()}

        if "payload" in cols and "payload_json" not in cols:
            c.execute("ALTER TABLE ares_vr_cache ADD COLUMN payload_json TEXT")
            c.execute("UPDATE ares_vr_cache SET payload_json = payload WHERE payload_json IS NULL")
            conn.commit()
            cols.add("payload_json")

        if "updated_at" in cols and "fetched_at" not in cols:
            c.execute("ALTER TABLE ares_vr_cache ADD COLUMN fetched_at TEXT")
            c.execute("UPDATE ares_vr_cache SET fetched_at = updated_at WHERE fetched_at IS NULL")
            conn.commit()
            cols.add("fetched_at")

        if "fetched_at" not in cols:
            c.execute("ALTER TABLE ares_vr_cache ADD COLUMN fetched_at TEXT")
            c.execute("UPDATE ares_vr_cache SET fetched_at = COALESCE(fetched_at, DATETIME('now'))")
            conn.commit()

        c.execute("CREATE INDEX IF NOT EXISTS idx_ares_vr_cache_fetched_at ON ares_vr_cache(fetched_at)")
        conn.commit()

    finally:
        conn.close()

# Získání cesty k DB + inicializace
try:
    from importer.pipeline import DB_PATH
    ares_db_path = str(DB_PATH)
except Exception:
    ares_db_path = str(Path("data") / "ares_vr_cache.sqlite")

# Volitelný override přes env proměnnou
ares_db_path = os.environ.get("ARES_CACHE_PATH", ares_db_path)
ensure_ares_cache_db(ares_db_path, schema_path="db/schema.sql")

# ======== ESM PDF Parsing (pomocné funkce) — MULTILINE + tituly bez tečky ========
from PyPDF2 import PdfReader

# Doplněné prefixy – varianty bez tečky i s mezerou
TITLES_PREFIX = [
    "Ing.", "Ing", "Ing. arch.", "Ing arch", "Ing.arch.",
    "Mgr.", "Mgr",
    "Bc.", "Bc",
    "JUDr.", "JUDr",
    "MUDr.", "MUDr",
    "PhDr.", "PhDr",
    "RNDr.", "RNDr",
    "doc.", "doc", "Doc.", "Doc",
    "prof.", "prof", "Prof.", "Prof",
    "PhMr.", "PhMr",
    "MDDr.", "MDDr",
    "MVDr.", "MVDr",
    "ThDr.", "ThDr",
    "ThLic.", "ThLic",
]
TITLES_SUFFIX = [
    "MBA", "LL.M.", "LL.M", "Ph.D.", "PhD", "DiS.", "DiS",
    "CSc.", "CSc", "DBA", "MSc.", "MSc", "BA", "BBA",
    "LLB", "MA", "ACCA", "CFA"
]

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def _remove_titles(name: str) -> str:
    s = (name or "").strip()
    # odstranit suffixy za čárkou nebo na konci
    s = re.sub(r",\s*(" + "|".join([re.escape(t) for t in TITLES_SUFFIX]) + r")\b\.?", "", s, flags=re.IGNORECASE)
    for t in TITLES_SUFFIX:
        s = re.sub(r"\b" + re.escape(t) + r"\b\.?", "", s, flags=re.IGNORECASE)
    # odstranit prefixy na začátku (s i bez tečky)
    for t in TITLES_PREFIX:
        s = re.sub(r"^\s*" + re.escape(t) + r"\b\.?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _norm_name_person(s: str) -> str:
    s = (s or "").strip()
    s = _remove_titles(s)
    s = _strip_accents(s).lower()
    s = re.sub(r"\s+", " ", s)
    return s

def _parse_pct_num(s: str) -> float | None:
    if not s: return None
    try:
        v = float(s.replace(",", ".").replace(";", "."))
        if v < 0: return None
        return v / 100.0
    except Exception:
        return None

# --- labely + sběr víc řádků za labelem ---
_LABEL_TOKENS = [
    "skutecny majitel",
    "statni prislusnost",
    "adresa",
    "povaha postaveni skutecneho majitele",
    "povaha skutecneho majitele",
    "udaje o skutecnostech",
    "neprimy podil",
    "podil na hlasovacich pravech",
    "rozhodujici vliv",
    "jina skutecnost",
    "podil - velikost podilu",
    "podil velikost podilu",
    # jméno na dva řádky:
    "jmeno",
    "prijmeni",
    # často se v textu vyskytuje i obecný label:
    "textovy popis",
    "textový popis",
    "jednani ve shode",
    "jednani ve shode s",
    "jednání ve shodě",
    "jednání ve shodě s",
]

def _norm(s: str) -> str:
    return _strip_accents((s or "").replace("–", "-").replace("—", "-")).lower().strip()

def _is_label(line: str) -> bool:
    ln = _norm(line)
    for tok in _LABEL_TOKENS:
        if ln.startswith(tok):
            return True
    return False

def _collect_after_label_multiline(lines: list[str], start_idx: int) -> str:
    """
    Vrátí text na stejném nebo následných řádcích za labelem až po další label / prázdný řádek / konec bloku.
    """
    cur = lines[start_idx]
    val = ""
    if ":" in cur:
        parts = cur.split(":", 1)
        tail = parts[1].strip()
        if tail:
            val = tail

    i = start_idx + 1
    collected = []
    while i < len(lines):
        s = lines[i].strip()
        if not s:
            break
        if _is_label(s):
            break
        collected.append(s)
        i += 1

    if collected:
        if val:
            return (val + " " + " ".join(collected)).strip()
        return " ".join(collected).strip()
    return val.strip()

def extract_esm_owners_from_pdf(pdf_bytes: bytes) -> list[dict]:
    """
    Z PDF ESM vytáhne záznamy „Skutečný majitel“ (ignoruje čistě historické bloky jen s 'vymazáno ...'):
      name, povaha, neprimy_podil(0..1), hlasovaci_podil(0..1), vliv_podil(0..1), shoda_s, jina
    """
    reader = PdfReader(BytesIO(pdf_bytes))
    full_text_parts = []
    for p in reader.pages:
        try:
            full_text_parts.append(p.extract_text() or "")
        except Exception:
            full_text_parts.append("")
    full_text = "\n".join(full_text_parts)

    # normalizace/čištění artefaktů z PDF
    full_text = re.sub(r"[*_`]+", "", full_text)
    full_text = full_text.replace("–", "-").replace("—", "-")
    full_text = re.sub(r"[ \t]+", " ", full_text)
    full_text = re.sub(r"\n\s+\n", "\n\n", full_text)

    # accent-insensitive kopie pro hledání sekcí
    full_text_nrm = _strip_accents(full_text).lower()

    # pokus najít začátek sekce (fallback: celý dokument)
    start_idx = full_text_nrm.find("skutecni majitele")
    if start_idx == -1:
        start_idx = full_text_nrm.find("skutecni majitelé")
    tail = full_text if start_idx == -1 else full_text[start_idx:]

    # konec sekce (pokud jsme sekci našli)
    if start_idx != -1:
        tail_nrm = full_text_nrm[start_idx:]
        m_end = re.search(r"\n(?:struktura vztahu|poznamky|zakladni identifikace|historie|zapisy)\b", tail_nrm, re.IGNORECASE)
        if m_end:
            end_offset = m_end.start()
            tail = tail[:end_offset]

    # rozděl bloky podle "Skutečný majitel"
    blocks = re.split(r"\n\s*Skutečný majitel[^\n]*\n", tail, flags=re.IGNORECASE)
    if len(blocks) <= 1:
        blocks = re.split(r"Skutečný majitel[^\n]*", tail, flags=re.IGNORECASE)

    owners = []
    for blk in blocks:
        blk_stripped = blk.strip()
        if not blk_stripped:
            continue

        # přeskoč čistě historické bloky (jen 'vymazáno' bez 'zapsáno')
        if re.search(r"\bvymazáno\b", blk_stripped, re.IGNORECASE) and not re.search(r"\bzapsáno\b", blk_stripped, re.IGNORECASE):
            continue

        lines = [l.rstrip() for l in blk_stripped.splitlines() if l.strip()]
        if not lines:
            continue

        # ===== Jméno =====
        name = None

        # 1) "Skutečný majitel: [automaticky propsáno] XY , ..."
        m = re.search(
            r"Skutečný\s+majitel\s*[:\-]\s*(?:automaticky\s+propsáno\s*)?(.+?)\s*(?:,|$|\n)",
            blk_stripped, re.IGNORECASE
        )
        if m:
            name = _remove_titles(m.group(1).strip())

        # 2) "Jméno a příjmení: XY"
        if not name:
            m2 = re.search(r"Jméno a příjmení\s*[:\-]\s*(.+)", blk_stripped, re.IGNORECASE)
            if m2:
                name = _remove_titles(m2.group(1).splitlines()[0].strip())

        # 3) kombinace "Jméno:" + "Příjmení:"
        if not name:
            first_name = None
            last_name = None
            for i, s in enumerate(lines):
                if _norm(s).startswith("jmeno"):
                    first_name = _collect_after_label_multiline(lines, i) or None
                if _norm(s).startswith("prijmeni"):
                    last_name = _collect_after_label_multiline(lines, i) or None
            if first_name and last_name:
                name = _remove_titles(f"{first_name.strip()} {last_name.strip()}")

        # 4) fallback – první smysluplný řádek
        if not name:
            for cand in lines[:3]:
                if not re.match(r"^(Státní příslušnost|Povaha|Údaje o skutečnostech|Adresa|Datum narození|Jednání ve shodě|Jiná skutečnost)\b", cand, re.IGNORECASE):
                    name = _remove_titles(cand.split(",")[0].strip())
                    break

        # ===== Povaha (vezmi první klauzuli / větu) =====
        povaha = None
        for i, s in enumerate(lines):
            ns = _norm(s)
            if ns.startswith("povaha postaveni skutecneho majitele") or ns.startswith("povaha skutecneho majitele"):
                val = _collect_after_label_multiline(lines, i)
                if val:
                    povaha = val.strip().split(". ")[0].strip()
                break

        # ===== Nepřímý podíl – velikost podílu: X % (multiline) =====
        neprimy = None
        for i, s in enumerate(lines):
            if _norm(s).startswith("neprimy podil"):
                val = _collect_after_label_multiline(lines, i)
                m_pct = re.search(r"(?:velikost\s+pod[ií]lu\s*[:\-]\s*)?([0-9]+(?:[.,;]\d+)?)\s*%", val, re.IGNORECASE)
                if m_pct:
                    neprimy = _parse_pct_num(m_pct.group(1))
                break

        # ===== Podíl na hlasovacích právech (%) =====
        hlas = None
        for i, s in enumerate(lines):
            if _norm(s).startswith("podil na hlasovacich pravech"):
                val = _collect_after_label_multiline(lines, i)
                m_pct = re.search(r"([0-9]+(?:[.,;]\d+)?)\s*%", val)
                if m_pct:
                    hlas = _parse_pct_num(m_pct.group(1))
                break

        # ===== Obecný "Podíl - velikost podílu" (přímý SM) =====
        if hlas is None:
            for i, s in enumerate(lines):
                ns = _norm(s)
                if ns.startswith("podil - velikost podilu") or ns.startswith("podil velikost podilu") or re.match(r"^podil\b", ns):
                    val = _collect_after_label_multiline(lines, i)
                    m_pct = re.search(r"([0-9]+(?:[.,;]\d+)?)\s*%", val)
                    if m_pct and not any(_norm(x).startswith("neprimy podil") for x in lines[max(0,i-1):i+2]):
                        hlas = _parse_pct_num(m_pct.group(1))
                    break

        # ===== Rozhodující vliv … velikost podílu =====
        vliv_podil = None
        for i, s in enumerate(lines):
            if _norm(s).startswith("rozhodujici vliv"):
                val = _collect_after_label_multiline(lines, i)
                m_pct = re.search(r"([0-9]+(?:[.,;]\d+)?)\s*%", val)
                if m_pct:
                    vliv_podil = _parse_pct_num(m_pct.group(1))
                break

        # ===== Jednání ve shodě =====
        shoda_s = None
        for i, s in enumerate(lines):
            ns = _norm(s)
            if ns.startswith("jednani ve shode"):
                val = _collect_after_label_multiline(lines, i)
                if val:
                    shoda_s = val.strip()
                break
        if not shoda_s:
            for i, s in enumerate(lines):
                ns = _norm(s)
                if ns.startswith("textovy popis") or ns.startswith("textový popis"):
                    val = _collect_after_label_multiline(lines, i)
                    m_shoda = re.search(r"jedn[aá]no?\s+ve\s+shod[ěe]\s+s\s*[:\-]?\s*(.+?)(?:;|$)", val, re.IGNORECASE)
                    if m_shoda:
                        shoda_s = m_shoda.group(1).strip()
                        break

        # ===== Jiná skutečnost (může být víc řádků) =====
        jina = None
        for i, s in enumerate(lines):
            if _norm(s).startswith("jina skutecnost"):
                val = _collect_after_label_multiline(lines, i)
                jina = val.strip() if val else None
                break

        # Bez rozumného jména přeskoč
        if not name or re.match(r"^(Skuteční majitelé|Státní příslušnost)\b", name, re.IGNORECASE):
            continue

        owners.append({
            "name": name,
            "povaha": povaha,
            "neprimy_podil": neprimy,
            "hlasovaci_podil": hlas,
            "vliv_podil": vliv_podil,
            "shoda_s": shoda_s,
            "jina": jina,
            "raw_block": blk_stripped,
        })
    return owners

# ===== UBO – parsování textových podílů =====
PCT_RE = re.compile(r"(\d+(?:[.,;]\d+)?)\s*%")
PROCENTA_RE = re.compile(r"(\d+(?:[.,;]\d+)?)\s*PROCENTA", re.IGNORECASE)
FRAC_SLASH_RE = re.compile(r"(\d+)\s*/\s*(\d+)")
FRAC_SEMI_RE = re.compile(r"(\d+)\s*;\s*(\d+)\s*(ZLOMEK|TEXT)?", re.IGNORECASE)
OBCHODNI_PODIL_FRAC_RE = re.compile(r"obchodni[_ ]?podil\s*:\s*(\d+)\s*[/;]\s*(\d+)", re.IGNORECASE)
OBCHODNI_PODIL_PCT_RE = re.compile(r"obchodni[_ ]?podil\s*:\s*(\d+(?:[.,;]\d+)?)\s*(?:%|PROCENTA)", re.IGNORECASE)
HLASOVACI_PRAVA_PCT_RE = re.compile(r"hlasovaci[_ ]?prava\s*:\s*(\d+(?:[.,;]\d+)?)\s*(?:%|PROCENTA)", re.IGNORECASE)
SPLACENO_FIELD_RE = re.compile(r"splaceno\s*:\s*\d+(?:[.,;]\d+)?\s*PROCENTA", re.IGNORECASE)
EFEKTIVNE_RE = re.compile(r"efektivně\s+(\d+(?:[.,;]\d+)?)\s*%", re.IGNORECASE)

def _to_float(s: str) -> float | None:
    try:
        return float(s.replace(",", ".").replace(";", "."))
    except Exception:
        return None

def parse_pct_from_text(s: str) -> float | None:
    s = (s or "").strip()
    if not s:
        return None
    s = SPLACENO_FIELD_RE.sub("", s)

    total = 0.0
    found = False
    for m in OBCHODNI_PODIL_FRAC_RE.finditer(s):
        a = _to_float(m.group(1)); b = _to_float(m.group(2))
        if a is not None and b and b != 0:
            total += (a / b); found = True
    for m in OBCHODNI_PODIL_PCT_RE.finditer(s):
        v = _to_float(m.group(1))
        if v is not None:
            total += (v / 100.0); found = True
    if found:
        return max(0.0, min(1.0, total))

    hv_total = 0.0; hv_found = False
    for m in HLASOVACI_PRAVA_PCT_RE.finditer(s):
        v = _to_float(m.group(1))
        if v is not None:
            hv_total += (v / 100.0); hv_found = True
    if hv_found:
        return max(0.0, min(1.0, hv_total))

    frac_total = 0.0; frac_found = False
    for m in FRAC_SLASH_RE.finditer(s):
        a = _to_float(m.group(1)); b = _to_float(m.group(2))
        if a is not None and b and b != 0:
            frac_total += (a / b); frac_found = True
    for m in FRAC_SEMI_RE.finditer(s):
        a = _to_float(m.group(1)); b = _to_float(m.group(2))
        if a is not None and b and b != 0:
            frac_total += (a / b); frac_found = True
    if frac_found:
        return max(0.0, min(1.0, frac_total))

    pct_total = 0.0; pct_found = False
    for m in PCT_RE.finditer(s):
        v = _to_float(m.group(1))
        if v is not None:
            pct_total += (v / 100.0); pct_found = True
    for m in PROCENTA_RE.finditer(s):
        v = _to_float(m.group(1))
        if v is not None:
            pct_total += (v / 100.0); pct_found = True
    if pct_found:
        return max(0.0, min(1.0, pct_total))

    return None

# ===== Výpočet efektivních podílů + diagnostika =====
def compute_effective_persons(lines) -> dict[str, dict]:
    """
    Spočte efektivní podíly fyzických osob a přidá diagnostiku cest.
    - násobení napříč patry: stack hlaviček + pending multiplikátor pro nejbližší dětskou hlavičku,
    - u firmy preferuje NodeLine.effective_pct k odvození local_share,
    - sčítání napříč větvením, voting default = kapitál.
    """
    persons: dict[str, dict] = {}

    header_stack: list[tuple[int, float]] = []   # [(header_depth, multiplier)]
    pending_next_header_mult: float | None = None

    for ln in _ensure_list(lines):
        depth, t = _line_depth_text(ln)
        if not t:
            continue

        # HLAVIČKA FIRMY
        if RE_COMPANY_HEADER.match(t):
            while header_stack and header_stack[-1][0] >= depth:
                header_stack.pop()
            parent_mult = header_stack[-1][1] if header_stack else 1.0
            this_mult = pending_next_header_mult if pending_next_header_mult is not None else parent_mult
            pending_next_header_mult = None
            header_stack.append((depth, this_mult))
            continue

        # LABEL
        if t.endswith(":"):
            continue

        # VLASTNÍK
        parts = DASH_SPLIT.split(t, maxsplit=1)
        name = (parts[0] if parts else t).strip()
        is_company = ICO_IN_LINE.search(t) is not None

        # rodičovská HLAVIČKA na depth-2
        expected_parent_header_depth = max(0, depth - 2)
        while header_stack and header_stack[-1][0] > expected_parent_header_depth:
            header_stack.pop()
        parent_mult = header_stack[-1][1] if header_stack else 1.0
        parent_depth = header_stack[-1][0] if header_stack else 0

        node_eff = None
        if hasattr(ln, "effective_pct") and getattr(ln, "effective_pct") is not None:
            try:
                node_eff = float(getattr(ln, "effective_pct")) / 100.0
            except Exception:
                node_eff = None

        if is_company:
            local_share = None
            if node_eff is not None and parent_mult > 0:
                local_share = node_eff / parent_mult
            else:
                local_share = parse_pct_from_text(t)
                if local_share is None:
                    m = EFEKTIVNE_RE.search(t)
                    if m:
                        eff_pct = _to_float(m.group(1))
                        if eff_pct is not None and parent_mult > 0:
                            local_share = (eff_pct / 100.0) / parent_mult
            pending_next_header_mult = parent_mult * local_share if local_share is not None else None

        else:
            entry = persons.setdefault(name, {"ownership": 0.0, "voting": 0.0, "paths": [], "debug_paths": []})

            local_share = None
            eff = None
            src = None
            if node_eff is not None:
                eff = node_eff; src = "node_eff(person)"
            else:
                local_share = parse_pct_from_text(t)
                if local_share is not None:
                    eff = parent_mult * local_share; src = "text(person)"
                else:
                    m = EFEKTIVNE_RE.search(t)
                    if m:
                        eff_pct = _to_float(m.group(1))
                        if eff_pct is not None:
                            eff = eff_pct / 100.0; src = "efektivně_text(person)"

            if eff is not None:
                entry["ownership"] += eff
                entry["voting"] += eff
                entry["paths"].append((parent_depth, eff, t))
            else:
                entry["paths"].append((parent_depth, None, t))

            entry["debug_paths"].append({
                "parent_depth": parent_depth,
                "parent_mult": parent_mult,
                "local_share": local_share,
                "eff": eff,
                "source": src or "unknown",
                "text": t,
            })

    for v in persons.values():
        v["ownership"] = max(0.0, min(1.0, v["ownership"]))
        v["voting"]    = max(0.0, min(1.0, v["voting"]))
    return persons

def fmt_pct(x: float | None) -> str:
    if x is None:
        return "—"
    return f"{(x * 100.0):.2f}%"

# ===== PDF utils =====
def _draw_wrapped_string(c: canvas.Canvas, font_name: str, font_size: int, x: float, y: float, text: str, max_width: float):
    c.setFont(font_name, font_size)
    w = pdfmetrics.stringWidth(text, font_name, font_size)
    if w <= max_width:
        c.drawString(x, y, text); return 1
    cut = len(text)
    while cut > 0 and pdfmetrics.stringWidth(text[:cut], font_name, font_size) > max_width:
        cut = text.rfind(" ", 0, cut)
        if cut == -1: break
    if cut > 0:
        line1 = text[:cut].rstrip()
        line2 = text[cut:].lstrip()
        c.drawString(x, y, line1)
        c.drawString(x, y - (font_size + 2), line2)
        return 2
    approx = int(max_width / (font_size * 0.55))
    c.drawString(x, y, text[:approx])
    c.drawString(x, y - (font_size + 2), text[approx:])
    return 2

def build_pdf(
    text_lines: list[str],
    graph_png_bytes: bytes | None,
    logo_bytes: bytes | None,
    company_links: list[tuple[str, str]],
    ubo_lines: list[str] | None = None,
) -> bytes:
    from reportlab.pdfgen import canvas as _canvas
    from reportlab.pdfbase import pdfmetrics as _pdfmetrics
    buf = BytesIO()
    c = _canvas.Canvas(buf, pagesize=A4)
    PAGE_W, PAGE_H = A4
    MARGIN = 36

    c.setFont(PDF_FONT_NAME, 10)

    y_top = PAGE_H - MARGIN
    text_x = MARGIN
    title_font = 16

    if logo_bytes:
        try:
            img = ImageReader(BytesIO(logo_bytes))
            ow, oh = img.getSize()
            target_w = 160.0
            scale = target_w / float(ow)
            target_h = oh * scale
            c.drawImage(img, MARGIN, y_top - target_h, width=target_w, height=target_h, preserveAspectRatio=True, mask='auto')
            text_x = MARGIN + target_w + 12
            logo_bottom_y = y_top - target_h
        except Exception:
            logo_bottom_y = y_top
    else:
        logo_bottom_y = y_top

    title = "MDG UBO Tool - AML kontrola vlastnické struktury na ARES"
    available_w = PAGE_W - MARGIN - text_x
    _draw_wrapped_string(c, PDF_FONT_NAME, title_font, text_x, y_top - title_font, title, available_w)

    c.setFont(PDF_FONT_NAME, 10)
    c.drawString(MARGIN, 18, f"Časové razítko: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    start_y = logo_bottom_y - 12
    c.setFont(PDF_FONT_NAME, 12)
    c.drawString(MARGIN, start_y, "Textový výstup")
    c.setFont(PDF_FONT_NAME, 10)

    text_obj = c.beginText()
    text_obj.setTextOrigin(MARGIN, start_y - 18)
    text_obj.setLeading(14)

    for line in text_lines:
        s = line
        while len(s) > 95:
            cut = s.rfind(" ", 0, 95)
            if cut == -1: cut = 95
            text_obj.textLine(s[:cut]); s = s[cut:].lstrip()
            if text_obj.getY() < 140:
                c.drawText(text_obj); c.showPage()
                c.setFont(PDF_FONT_NAME, 10)
                text_obj = c.beginText()
                text_obj.setTextOrigin(MARGIN, PAGE_H - MARGIN - 40)
                text_obj.setLeading(14)
        text_obj.textLine(s)
        if text_obj.getY() < 140:
            c.drawText(text_obj); c.showPage()
            c.setFont(PDF_FONT_NAME, 10)
            text_obj = c.beginText()
            text_obj.setTextOrigin(MARGIN, PAGE_H - MARGIN - 40)
            text_obj.setLeading(14)
    c.drawText(text_obj)

    if graph_png_bytes:
        c.showPage()
        c.setFont(PDF_FONT_NAME, 12)
        c.drawString(MARGIN, PAGE_H - MARGIN - 20, "Grafická struktura")
        try:
            img = ImageReader(BytesIO(graph_png_bytes))
            IMG_MAX_W = PAGE_W - 2 * MARGIN
            IMG_MAX_H = PAGE_H - 2 * MARGIN - 40
            c.drawImage(img, MARGIN, MARGIN, width=IMG_MAX_W, height=IMG_MAX_H, preserveAspectRatio=True, anchor='sw', mask='auto')
        except Exception:
            c.setFont(PDF_FONT_NAME, 10)
            c.drawString(MARGIN, PAGE_H - MARGIN - 40, "⚠️ Nelze vložit obrázek grafu do PDF.")

    if company_links:
        c.showPage()
        c.setFont(PDF_FONT_NAME, 12)
        c.drawString(MARGIN, PAGE_H - MARGIN - 20, "ODKAZY NA OR")
        c.setFont(PDF_FONT_NAME, 10)
        y_links = PAGE_H - MARGIN - 40
        for name, url in company_links:
            line_text = f"{name} — {url}"
            c.drawString(MARGIN, y_links, line_text)
            name_part = f"{name} — "
            url_x = MARGIN + _pdfmetrics.stringWidth(name_part, PDF_FONT_NAME, 10)
            url_w = _pdfmetrics.stringWidth(url, PDF_FONT_NAME, 10)
            c.linkURL(url, (url_x, y_links - 2, url_x + url_w, y_links + 10), relative=0)
            y_links -= 16
            if y_links < MARGIN + 40:
                c.showPage(); c.setFont(PDF_FONT_NAME, 10)
                y_links = PAGE_H - MARGIN - 40

    if ubo_lines:
        c.showPage()
        c.setFont(PDF_FONT_NAME, 12)
        c.drawString(MARGIN, PAGE_H - MARGIN - 20, "Skuteční majitelé (vyhodnocení)")
        c.setFont(PDF_FONT_NAME, 10)
        y = PAGE_H - MARGIN - 40
        for line in ubo_lines:
            if len(line) <= 120:
                c.drawString(MARGIN, y, line); y -= 14
            else:
                s = line
                while len(s) > 0:
                    cut = s.rfind(" ", 0, 120)
                    if cut == -1: cut = min(120, len(s))
                    c.drawString(MARGIN, y, s[:cut]); y -= 14
                    s = s[cut:].lstrip()
                    if y < MARGIN + 40:
                        c.showPage(); c.setFont(PDF_FONT_NAME, 10)
                        y = PAGE_H - MARGIN - 40
            if y < MARGIN + 40:
                c.showPage(); c.setFont(PDF_FONT_NAME, 10)
                y = PAGE_H - MARGIN - 40

    c.save()
    return buf.getvalue()

# ===== Header =====
title_html = f"""
<div class="header-row">
  {'<img class="logo" src="' + data_uri + '"/>' if data_uri else ''}
  <h2>MDG UBO Tool - AML kontrola vlastnické struktury na ARES</h2>
</div>
<div class="header-caption">Online režim: společníci/akcionáři se načítají z ARES VR API (např. /ekonomicke-subjekty-vr/{{ICO}}).</div>
"""
st.markdown(title_html, unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

# ===== UI vstupy =====
ico = st.text_input("IČO společnosti", value="", placeholder="např. 03999840")
max_depth = st.slider("Max. hloubka rozkrytí", 1, 60, 25, 1)

col1, col2 = st.columns([1, 3])
with col1:
    run = st.button("🔎 Rozkrýt strukturu", type="primary")
with col2:
    st.write("")

# ===== Session state =====
if "last_result" not in st.session_state:
    st.session_state["last_result"] = None
if "ubo_overrides" not in st.session_state:
    st.session_state["ubo_overrides"] = {}      # jméno -> 0..1 (hlasovací)
if "ubo_cap_overrides" not in st.session_state:
    st.session_state["ubo_cap_overrides"] = {}  # jméno -> 0..1 (kapitál)
if "manual_persons" not in st.session_state:
    st.session_state["manual_persons"] = {}     # {name: {"cap","vote","veto","org_majority","substitute_ubo"}}
if "final_persons" not in st.session_state:
    st.session_state["final_persons"] = None
if "esm_owners_pdf" not in st.session_state:
    st.session_state["esm_owners_pdf"] = None
if "esm_debug_text" not in st.session_state:
    st.session_state["esm_debug_text"] = None
# >>> Ruční vlastníci firem pro OR override <<<
if "manual_company_owners" not in st.session_state:
    # {target_company_ico: [{"ico": "XXXXXXXX", "share": 0..1}, ...]}
    st.session_state["manual_company_owners"] = {}

# ===== Akce: Rozkrýt =====
if run:
    if not ico.strip():
        st.error("Zadej IČO."); st.stop()

    cb = progress_ui(); cb("Start…", 0.01)
    try:
        client = AresVrClient(ares_db_path)
        cb("Načítám z ARES a rozkrývám…", 0.10)

        # Předání ručních override do resolve
        manual_overrides = {
            k: [(item["ico"], item["share"]) for item in v]
            for k, v in st.session_state["manual_company_owners"].items()
        }
        res = resolve_tree_online(
            client=client,
            root_ico=ico.strip(),
            max_depth=int(max_depth),
            manual_overrides=manual_overrides,
        )
        lines, warnings = _normalize_resolve_result(res)
        cb("Hotovo.", 1.0)

        rendered = render_lines(lines)
        # Graf (renderer interně skrývá hlavičku „Manuálně doplněno:“)
        g = build_graphviz_from_nodelines_bfs(
            lines,
            root_ico=ico.strip(),
            title=f"Ownership_{ico.strip()}",
        )

        graph_png = None
        try:
            graph_png = g.pipe(format="png")
        except Exception:
            graph_png = None

        companies = extract_companies_from_lines(lines)

        # Reset overrides a manuálních osob, aby se nepřenášely mezi firmami
        st.session_state["ubo_overrides"].clear()
        st.session_state["ubo_cap_overrides"].clear()
        st.session_state["manual_persons"].clear()
        st.session_state["final_persons"] = None
        st.session_state["esm_owners_pdf"] = None
        st.session_state["esm_debug_text"] = None

        st.session_state["last_result"] = {
            "lines": lines,
            "warnings": warnings,
            "graphviz": g,
            "graph_png": graph_png,
            "text_lines": rendered,
            "companies": companies,
            "ubo_pdf_lines": None,
            "unresolved": [w for w in warnings if isinstance(w, dict) and w.get("kind") == "unresolved"],
        }
        st.success("Struktura byla načtena. Níže se zobrazí výsledky.")
    except Exception as e:
        st.error("Spadlo to na chybě:"); st.code(str(e))

# ===== Persistentní render =====
lr = st.session_state.get("last_result")
if lr:
    st.subheader("Výsledek (Text)")
    st.caption("Odsazení = úroveň. Každý blok: firma → její společníci/akcionáři.")
    st.code("\n".join(lr["text_lines"]), language="text")

    st.subheader("Výsledek (Graf)")
    try:
        st.graphviz_chart(lr["graphviz"].source)
    except Exception:
        st.warning("Nelze zobrazit graf (Graphviz).")

    # ===== Manuální doplnění vlastníků (firmy bez dohledaných vlastníků) =====
    st.subheader("Doplnění vlastníků u firem bez dohledaných společníků/akcionářů")
    st.caption("Vyber firmu bez vlastníků (OR) a doplň její vlastníky (IČO + podíl). Po přidání se struktura rekurzivně rozbalí až k FO.")

    unresolved_list = st.session_state.get("last_result", {}).get("unresolved") or []
    if not unresolved_list:
        st.info("V aktuální struktuře nejsou firmy bez dohledaných vlastníků.")
    else:
        opts = [f"{u.get('name','?')} (IČO {str(u.get('ico') or '').zfill(8)})" for u in unresolved_list]
        picked = st.selectbox("Firma k doplnění", options=opts, index=0)
        picked_idx = opts.index(picked) if picked in opts else 0
        target_ico = str(unresolved_list[picked_idx].get("ico") or "").zfill(8)
        target_name = unresolved_list[picked_idx].get("name") or "Neznámá firma"

        st.markdown("**Zadej vlastníky (IČO a podíl v %)** — formát: `ICO1: 50, ICO2: 50`")
        owners_raw = st.text_input("Seznam vlastníků (IČO: %, oddělit čárkou)", placeholder="03999840: 50, 17947103: 50")

        add_btn = st.button("➕ Přidat do vlastnické struktury (manuálně)")
        if add_btn:
            # pomocná funkce na parsování
            def _parse_pairs(s: str):
                out = []
                for chunk in (s or "").split(","):
                    chunk = chunk.strip()
                    if not chunk:
                        continue
                    if ":" not in chunk:
                        st.error(f"Nesprávný formát: „{chunk}“ — očekáván „IČO: %“")
                        return None
                    ico_part, pct_part = chunk.split(":", 1)
                    ico_clean = re.sub(r"\D", "", ico_part).zfill(8)
                    if not ico_clean or not ico_clean.isdigit() or len(ico_clean) != 8:
                        st.error(f"Neplatné IČO: „{ico_part}“")
                        return None
                    try:
                        pct = float(pct_part.replace(",", ".").strip())
                    except Exception:
                        st.error(f"Neplatné procento: „{pct_part}“")
                        return None
                    if pct <= 0:
                        st.error(f"Podíl musí být > 0: „{pct}“")
                        return None
                    out.append({"ico": ico_clean, "share": pct / 100.0})
                return out

            parsed = _parse_pairs(owners_raw)
            if parsed is not None and parsed:
                total = sum(p["share"] for p in parsed)
                if total > 1.0 + 1e-6:
                    st.warning(f"Součet podílů {total*100.0:.2f}% > 100% — pokračuji, ale zvaž úpravu.")

                # uložit overrides pro cílovou firmu
                st.session_state["manual_company_owners"][target_ico] = parsed

                # re-resolve s manuálními vlastníky
                try:
                    client = AresVrClient(ares_db_path)
                    manual_overrides = {
                        k: [(item["ico"], item["share"]) for item in v]
                        for k, v in st.session_state["manual_company_owners"].items()
                    }
                    res2 = resolve_tree_online(
                        client=client,
                        root_ico=ico.strip(),
                        max_depth=int(max_depth),
                        manual_overrides=manual_overrides,
                    )
                    lines2, warnings2 = _normalize_resolve_result(res2)
                    rendered2 = render_lines(lines2)
                    g2 = build_graphviz_from_nodelines_bfs(
                        lines2,
                        root_ico=ico.strip(),
                        title=f"Ownership_{ico.strip()}",
                    )

                    graph_png2 = None
                    try:
                        graph_png2 = g2.pipe(format="png")
                    except Exception:
                        graph_png2 = None

                    companies2 = extract_companies_from_lines(lines2)

                    st.session_state["last_result"] = {
                        "lines": lines2,
                        "warnings": warnings2,
                        "graphviz": g2,
                        "graph_png": graph_png2,
                        "text_lines": rendered2,
                        "companies": companies2,
                        "ubo_pdf_lines": st.session_state["last_result"].get("ubo_pdf_lines"),
                        "unresolved": [w for w in warnings2 if isinstance(w, dict) and w.get("kind") == "unresolved"],
                    }
                    st.success(f"Přidáno: {target_name} (IČO {target_ico}) — vlastníci doplněni, struktura znovu rozkryta.")
                    # vynutit okamžitý refresh UI po přidání manuálních vlastníků
                    st.rerun()
                except Exception as e:
                    st.error(f"Re‑resolve s manuálními vlastníky selhal: {e}")

    st.subheader("ODKAZY NA OR")
    companies = lr["companies"]
    if not companies:
        st.info("Nebyla nalezena žádná právnická osoba s IČO.")
    else:
        for name, ico_val in companies:
            url = f"https://or.justice.cz/ias/ui/rejstrik-$firma?ico={ico_val}&jenPlatne=VSECHNY"
            st.markdown(f"- **{name}** — {url}")

    company_links_now = [(name, f"https://or.justice.cz/ias/ui/rejstrik-$firma?ico={ico_val}&jenPlatne=VSECHNY") for name, ico_val in companies]
    pdf_bytes_now = build_pdf(
        text_lines=lr["text_lines"],
        graph_png_bytes=lr["graph_png"],
        logo_bytes=logo_bytes,
        company_links=company_links_now,
        ubo_lines=None,
    )
    st.download_button(
        label="📄 Generovat do PDF (bez vyhodnocení SM)",
        data=pdf_bytes_now,
        file_name=f"ownership_{ico.strip() or 'export'}.pdf",
        mime="application/pdf",
        type="primary",
    )

    # ===== SKUTEČNÍ MAJITELÉ (dle OR) =====
    st.subheader("SKUTEČNÍ MAJITELÉ (dle OR)")
    st.caption("Automatický přepočet textových podílů, násobení napříč patry (včetně využití `effective_pct`), sčítání větvení. Úpravy ZK/HP v %, právo veta, „jmenuje/odvolává většinu orgánu“, náhradní SM (§ 5 ZESM) a voting block. Práh je striktně > nastavené hodnoty.")

    persons = compute_effective_persons(lr["lines"])

    # Diagnostika výpočtu (volitelně)
    show_debug = st.checkbox("Zobrazit diagnostiku výpočtu (cesty a násobení)", value=False)
    if show_debug:
        st.info("Diagnostika: pro každou osobu jsou uvedeny jednotlivé cesty s multiplikátorem rodiče, lokálním podílem a efektivním příspěvkem.")
        for name, info in persons.items():
            st.markdown(f"**{name}** — efektivní kapitál: {fmt_pct(info['ownership'])}, hlasovací práva: {fmt_pct(info['voting'])}")
            dps = info.get("debug_paths", [])
            if not dps:
                st.caption("Bez diagnostických záznamů.")
                continue
            for i, dp in enumerate(dps, 1):
                def _fmt(x):
                    return "—" if x is None else f"{x*100.0:.2f}%"
                pm = _fmt(dp.get("parent_mult"))
                ls = _fmt(dp.get("local_share"))
                ef = _fmt(dp.get("eff"))
                src = dp.get("source") or "unknown"
                txt = dp.get("text") or ""
                st.markdown(
                    f"- cesta {i}: úroveň {dp.get('parent_depth', 0)}, "
                    f"multiplikátor rodiče: **{pm}**, lokální podíl: **{ls}**, "
                    f"efektivní příspěvek: **{ef}**; zdroj: `{src}`\n"
                    f"  \n  ↳ řádek: `{txt}`"
                )
            st.markdown("---")

    # Manuální doplnění osob (např. u akciové společnosti)
    st.markdown("**Manuální doplnění osob (např. u akciové společnosti):**")
    colM1, colM2, colM3, colM4, colM5, colM6, colM7 = st.columns([3, 2, 2, 2, 2, 2, 2])
    with colM1:
        manual_name = st.text_input("Jméno osoby (manuálně)", value="", key="manual_name")
    with colM2:
        manual_cap = st.number_input("Podíl na kapitálu (%)", min_value=0.0, max_value=100.0, value=0.0, step=0.01, key="manual_cap")
    with colM3:
        manual_vote = st.number_input("Hlasovací práva (%)", min_value=0.0, max_value=100.0, value=0.0, step=0.01, key="manual_vote")
    with colM4:
        manual_veto = st.checkbox("Právo veta", value=False, key="manual_veto")
    with colM5:
        manual_org_majority = st.checkbox("Jmenuje/odvolává většinu orgánu", value=False, key="manual_org_majority")
    with colM6:
        manual_substitute_ubo = st.checkbox("Náhradní skutečný majitel (§ 5 ZESM)", value=False, key="manual_substitute_ubo")
    with colM7:
        add_manual = st.button("➕ Přidat osobu manuálně", key="add_manual_btn")
    if add_manual and manual_name.strip():
        st.session_state["manual_persons"][manual_name.strip()] = {
            "cap": manual_cap / 100.0,
            "vote": manual_vote / 100.0,
            "veto": manual_veto,
            "org_majority": manual_org_majority,
            "substitute_ubo": manual_substitute_ubo,
        }
        st.success(
            f"Přidáno: {manual_name.strip()} (kapitál {manual_cap:.2f} %, "
            f"hlasovací {manual_vote:.2f} %, veto: {'ano' if manual_veto else 'ne'}, "
            f"jmenuje/odvolává většinu orgánu: {'ano' if manual_org_majority else 'ne'}, "
            f"náhradní SM (§ 5): {'ano' if manual_substitute_ubo else 'ne'})"
        )

    if st.session_state["manual_persons"]:
        st.markdown("**Manuálně přidané osoby:**")
        for mn, mi in list(st.session_state["manual_persons"].items()):
            colR1, colR2 = st.columns([6, 1])
            with colR1:
                st.markdown(
                    f"- **{mn}** — kapitál: {fmt_pct(mi['cap'])}, "
                    f"hlasovací práva: {fmt_pct(mi['vote'])}, "
                    f"veto: {'ano' if mi['veto'] else 'ne'}, "
                    f"jmenuje/odvolává většinu orgánu: {'ano' if mi['org_majority'] else 'ne'}, "
                    f"náhradní SM (§ 5): {'ano' if mi.get('substitute_ubo') else 'ne'}"
                )
            with colR2:
                if st.button(f"🗑️ Odebrat ({mn})", key=f"del_{mn}"):
                    st.session_state["manual_persons"].pop(mn, None)
                    st.info(f"Odebráno: {mn}")

    # Form SM
    overrides_vote = st.session_state["ubo_overrides"]
    overrides_cap = st.session_state["ubo_cap_overrides"]

    with st.form("ubo_form"):
        threshold_pct = st.number_input(
            "Práh pro skutečného majitele (%)",
            min_value=0.0, max_value=100.0, value=25.00, step=0.01,
            help="Striktně > prahu (např. 25,01 %)."
        )

        st.write("**Osoby a jejich efektivní podíly (z OR) + možnost úprav:**")
        veto_flags: dict[str, bool] = {}
        org_majority_flags: dict[str, bool] = {}
        substitute_flags: dict[str, bool] = {}
        edited_voting_pct: dict[str, float] = {}
        edited_cap_pct: dict[str, float] = {}

        for idx, (name, info) in enumerate(persons.items()):
            colA, colB, colC, colD, colE = st.columns([2.8, 2.0, 2.0, 2.0, 2.2])
            with colA:
                st.markdown(f"- **{name}**")
                st.markdown(f"  • Podíl na kapitálu (efektivně): **{fmt_pct(info['ownership'])}**")
                st.markdown(f"  • Hlasovací práva (výchozí): **{fmt_pct(info['voting'])}**")
            with colB:
                cap_default = overrides_cap.get(name, info["ownership"]) * 100.0
                edited_cap_pct[name] = st.number_input(
                    f"Podíl na ZK (%) ({name})",
                    min_value=0.0, max_value=100.0,
                    value=float(f"{cap_default:.2f}"),
                    step=0.01,
                    key=f"cap_{idx}_{name}",
                )
            with colC:
                vote_default = overrides_vote.get(name, info["voting"]) * 100.0
                edited_voting_pct[name] = st.number_input(
                    f"Hlasovací práva (%) ({name})",
                    min_value=0.0, max_value=100.0,
                    value=float(f"{vote_default:.2f}"),
                    step=0.01,
                    key=f"vote_{idx}_{name}",
                )
            with colD:
                veto_flags[name] = st.checkbox(
                    f"Právo veta ({name})", value=False, key=f"veto_{idx}_{name}",
                )
                org_majority_flags[name] = st.checkbox(
                    f"Jmenuje/odvolává většinu orgánu ({name})", value=False, key=f"orgmaj_{idx}_{name}",
                )
            with colE:
                substitute_flags[name] = st.checkbox(
                    f"Náhradní SM (§ 5) ({name})", value=False, key=f"subs_{idx}_{name}",
                    help="Použij při naplnění § 5 ZESM (nelze určit SM / rozhodující vliv PO bez SM)."
                )

        st.divider()
        st.write("**Jednání ve shodě (voting block):**")
        all_names = list(set(list(persons.keys()) + list(st.session_state["manual_persons"].keys())))
        block_members = st.multiselect(
            "Vyber účastníky voting blocku",
            all_names,
            [],
            placeholder="např. Jan Novák",
        )
        block_name = st.text_input("Název voting blocku", value="Voting Block 1")

        submitted = st.form_submit_button("Vyhodnotit skutečné majitele")

    if submitted:
        # Ulož overrides
        for n, v in edited_voting_pct.items():
            overrides_vote[n] = v / 100.0
        for n, v in edited_cap_pct.items():
            overrides_cap[n] = v / 100.0

        # Slož finální osoby (OR + manuální)
        final_persons: dict[str, dict] = {}
        for n, info in persons.items():
            final_persons[n] = {
                "cap": overrides_cap.get(n, info["ownership"]),
                "vote": overrides_vote.get(n, info["voting"]),
                "veto": veto_flags.get(n, False),
                "org_majority": org_majority_flags.get(n, False),
                "substitute_ubo": substitute_flags.get(n, False),  # § 5 ZESM
            }
        for mn, mi in st.session_state["manual_persons"].items():
            final_persons[mn] = {
                "cap": mi["cap"],
                "vote": mi["vote"],
                "veto": mi["veto"],
                "org_majority": mi["org_majority"],
                "substitute_ubo": mi.get("substitute_ubo", False),
            }

        # Ulož pro ESM porovnání
        st.session_state["final_persons"] = final_persons

        # Součty 100 %
        total_cap = sum(max(0.0, min(1.0, v["cap"])) for v in final_persons.values())
        total_vote = sum(max(0.0, min(1.0, v["vote"])) for v in final_persons.values())
        TOL = 0.001
        cap_ok = abs(total_cap - 1.0) <= TOL
        vote_ok = abs(total_vote - 1.0) <= TOL
        miss_cap = (1.0 - total_cap) * 100.0
        miss_vote = (1.0 - total_vote) * 100.0

        if cap_ok:
            st.success(f"Součet podílů na ZK = {total_cap*100.0:.2f} % (OK)")
        else:
            st.warning(f"Součet podílů na ZK = {total_cap*100.0:.2f} % (chybí {max(0.0, miss_cap):.2f} % / přebytek {max(0.0, -miss_cap):.2f} %)")

        if vote_ok:
            st.success(f"Součet hlasovacích práv = {total_vote*100.0:.2f} % (OK)")
        else:
            st.warning(f"Součet hlasovacích práv = {total_vote*100.0:.2f} % (chybí {max(0.0, miss_vote):.2f} % / přebytek {max(0.0, -miss_vote):.2f} %)")

        # Voting block
        block_total = sum(final_persons.get(n, {"vote": 0.0})["vote"] for n in block_members) if block_members else 0.0

        # Pravidla SM (striktně > threshold)
        thr = (threshold_pct / 100.0)
        ubo: dict[str, dict] = {}
        reasons: dict[str, list[str]] = {}
        def add_reason(n: str, r: str):
            reasons.setdefault(n, []).append(r)

        for n, vals in final_persons.items():
            cap = vals["cap"]; vote = vals["vote"]
            veto = vals.get("veto", False)
            orgmaj = vals.get("org_majority", False)
            substitute = vals.get("substitute_ubo", False)  # § 5 ZESM
            is_ubo = False
            if cap > thr:
                is_ubo = True; add_reason(n, f"podíl na kapitálu {fmt_pct(cap)} > {threshold_pct:.2f}%")
            if vote > thr:
                is_ubo = True; add_reason(n, f"hlasovací práva {fmt_pct(vote)} > {threshold_pct:.2f}%")
            if veto:
                is_ubo = True; add_reason(n, "právo veta → rozhodující vliv")
            if orgmaj:
                is_ubo = True; add_reason(n, "jmenuje/odvolává většinu orgánu → rozhodující vliv")
            if substitute:
                is_ubo = True
                add_reason(n, "náhradní skutečný majitel (§ 5 ZESM)")
            if is_ubo:
                ubo[n] = {"cap": cap, "vote": vote, "veto": veto, "org_majority": orgmaj, "substitute_ubo": substitute}

        if block_members and block_total > thr:
            for n in block_members:
                if n in final_persons:
                    cap = final_persons[n]["cap"]; vote = final_persons[n]["vote"]
                    veto = final_persons[n]["veto"]; orgmaj = final_persons[n]["org_majority"]
                    substitute = final_persons[n].get("substitute_ubo", False)
                    ubo[n] = {"cap": cap, "vote": vote, "veto": veto, "org_majority": orgmaj, "substitute_ubo": substitute}
                    add_reason(n, f"účast v voting blocku „{block_name}“ s {fmt_pct(block_total)} > {threshold_pct:.2f}%")

        st.success("Vyhodnocení dokončeno.")
        if not ubo:
            st.info("Nebyly zjištěny fyzické osoby splňující definici skutečného majitele dle zadaných pravidel.")
        else:
            st.markdown("**Skuteční majitelé:**")
            ubo_report_lines = []
            if cap_ok:
                ubo_report_lines.append(f"Součet podílů na ZK: {total_cap*100.0:.2f}% (OK)")
            else:
                ubo_report_lines.append(
                    f"Součet podílů na ZK: {total_cap*100.0:.2f}% (⚠︎ chybí {max(0.0, miss_cap):.2f}% / přebytek {max(0.0, -miss_cap):.2f}%)"
                )
            if vote_ok:
                ubo_report_lines.append(f"Součet hlasovacích práv: {total_vote*100.0:.2f}% (OK)")
            else:
                ubo_report_lines.append(
                    f"Součet hlasovacích práv: {total_vote*100.0:.2f}% (⚠︎ chybí {max(0.0, miss_vote): .2f}% / přebytek {max(0.0, -miss_vote):.2f}%)"
                )
            for n, vals in ubo.items():
                rs = "; ".join(reasons.get(n, []))
                line_txt = f"- {n} — kapitál: {fmt_pct(vals['cap'])}, hlasovací práva: {fmt_pct(vals['vote'])} — {rs}"
                st.markdown(line_txt)
                ubo_report_lines.append(line_txt)

            st.session_state["last_result"]["ubo_pdf_lines"] = ubo_report_lines
            pdf_bytes_with_ubo = build_pdf(
                text_lines=lr["text_lines"],
                graph_png_bytes=lr["graph_png"],
                logo_bytes=logo_bytes,
                company_links=company_links_now,
                ubo_lines=ubo_report_lines,
            )
            st.download_button(
                label="📄 Generovat do PDF (včetně vyhodnocení SM a součtů)",
                data=pdf_bytes_with_ubo,
                file_name=f"ownership_ubo_{ico.strip() or 'export'}.pdf",
                mime="application/pdf",
                type="primary",
            )

    # ===== 3) ESM – Nahrání a porovnání =====
    st.subheader("Výpis z evidence skutečných majitelů (ESM) — nahrání a porovnání")
    st.caption("⚠️ V mlčenlivém režimu spouštěj aplikaci **lokálně/on‑prem**. Nahrané PDF se zpracuje pouze **v paměti** (neukládá se na disk).")

    # === A) Nahrání oficiálního výpisu ESM (PDF) ===
    with st.expander("A) Nahrání oficiálního výpisu ESM (PDF)", expanded=False):
        uploaded = st.file_uploader("Nahraj oficiální PDF výpis z ESM", type=["pdf"], key="esm_pdf_uploader")
        if uploaded:
            pdf_bytes = uploaded.read()
            try:
                esm_owners = extract_esm_owners_from_pdf(pdf_bytes)
            except Exception as e:
                esm_owners = []
                st.error(f"Nepodařilo se vytěžit ESM: {e}")

            if not esm_owners:
                st.warning("V PDF se nepodařilo najít sekci **„Skuteční majitelé“** nebo žádný záznam bez „vymazáno …“. Zkontroluj, že jde o správný výpis.")
                # debug výřez z PDF textu
                try:
                    from PyPDF2 import PdfReader as _Reader
                    _r = _Reader(BytesIO(pdf_bytes))
                    _txt = []
                    for _p in _r.pages:
                        try:
                            _txt.append(_p.extract_text() or "")
                        except Exception:
                            _txt.append("")
                    dbg = ("\n".join(_txt) or "")[:1200]
                    st.session_state["esm_debug_text"] = dbg
                    st.caption("Náhled (výřez) vytěženého textu z PDF pro diagnostiku:")
                    st.code(dbg, language="text")
                except Exception:
                    pass
            else:
                st.success(f"Nalezeno záznamů SM v ESM PDF: {len(esm_owners)}")

                # ZOBRAZIT JEN JMÉNA (OČÍSLOVANĚ)
                st.markdown("**Jména z ESM PDF:**")
                for i, o in enumerate(esm_owners, 1):
                    st.markdown(f"{i}. {o['name']}")

                # ulož pro porovnání
                st.session_state["esm_owners_pdf"] = esm_owners

    # === B) Porovnání: „Vyhodnocení“ ↔ „ESM PDF“ ===
    with st.expander("B) Porovnání: „Vyhodnocení“ ↔ „ESM PDF“", expanded=False):
        st.caption("Porovnává se **naše vyhodnocení** (OR) s **ESM** pouze podle **jmen** (bez podílů).")
        final_persons = st.session_state.get("final_persons")
        esm_pdf = st.session_state.get("esm_owners_pdf")

        if not final_persons:
            st.info("Nejprve klikni **Vyhodnotit skutečné majitele** výše.")
        elif not esm_pdf:
            st.info("Nejprve v kroku A nahraj a vytěž ESM PDF.")
        else:
            # normalizace jmen a odstraňování titulů / diakritiky (už zohledňuje 'Ing' i bez tečky)
            our_names = { _norm_name_person(n): n for n in final_persons.keys() }
            esm_names = { _norm_name_person(o["name"]): o["name"] for o in esm_pdf }

            missing_in_esm = [our_names[k] for k in our_names.keys() - esm_names.keys()]
            extra_in_esm   = [esm_names[k] for k in esm_names.keys() - our_names.keys()]

            if not missing_in_esm and not extra_in_esm:
                st.success("✅ Personální shoda: seznamy jmen odpovídají (nikdo nechybí ani nepřebývá).")
            else:
                if missing_in_esm:
                    st.warning("❗ Chybí v ESM (ale jsou v OR):")
                    for n in missing_in_esm:
                        st.markdown(f"- {n}")
                if extra_in_esm:
                    st.warning("❗ Přebytek v ESM (není v OR):")
                    for n in extra_in_esm:
                        st.markdown(f"- {n}")

    # ===== Upozornění =====
    if lr["warnings"] or lr.get("unresolved"):
        st.subheader("Upozornění")
        for w in lr["warnings"]:
            if hasattr(w, "text"):
                st.warning(str(getattr(w, "text", w)))
            elif isinstance(w, dict):
                st.warning(str(w.get("text", w)))
            else:
                st.warning(str(w))
