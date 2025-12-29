import os
import re
import sqlite3
from io import BytesIO
from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET

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

/* Header */
.small-muted {{ color: #666; font-size: 0.9rem; }}
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

logo_bytes, logo_mime = load_project_logo()

# ===== PDF FONT s diakritikou =====
def find_font_file() -> Path | None:
    # bere i font v rootu projektu (tvůj případ)
    candidates = [
        Path("DejaVuSans.ttf"),
        Path("assets") / "DejaVuSans.ttf",
        Path("fonts") / "DejaVuSans.ttf",
        Path("static") / "DejaVuSans.ttf",
        Path("DejaVuSans") / "DejaVuSans.ttf",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None

PDF_FONT_NAME = "DejaVuSans"
font_path = find_font_file()
if font_path:
    try:
        pdfmetrics.registerFont(TTFont(PDF_FONT_NAME, str(font_path)))
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
    if x is None:
        return []
    if isinstance(x, (list, tuple)):
        return list(x)
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
    if not db_path:
        return
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        c = conn.cursor()

        if schema_path and Path(schema_path).exists():
            sql = Path(schema_path).read_text(encoding="utf-8")
            c.executescript(sql)
            conn.commit()
        else:
            c.execute("""
                CREATE TABLE IF NOT EXISTS ares_vr_cache (
                    ico TEXT PRIMARY KEY,
                    fetched_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_ares_vr_cache_fetched_at ON ares_vr_cache(fetched_at)")
            conn.commit()

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

try:
    from importer.pipeline import DB_PATH
    ares_db_path = str(DB_PATH)
except Exception:
    ares_db_path = str(Path("data") / "ares_vr_cache.sqlite")

ares_db_path = os.environ.get("ARES_CACHE_PATH", ares_db_path)
ensure_ares_cache_db(ares_db_path, schema_path="db/schema.sql")

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
    persons: dict[str, dict] = {}

    header_stack: list[tuple[int, float]] = []   # [(header_depth, multiplier)]
    pending_next_header_mult: float | None = None

    for ln in _ensure_list(lines):
        depth, t = _line_depth_text(ln)
        if not t:
            continue

        if RE_COMPANY_HEADER.match(t):
            while header_stack and header_stack[-1][0] >= depth:
                header_stack.pop()
            parent_mult = header_stack[-1][1] if header_stack else 1.0
            this_mult = pending_next_header_mult if pending_next_header_mult is not None else parent_mult
            pending_next_header_mult = None
            header_stack.append((depth, this_mult))
            continue

        if t.endswith(":"):
            continue

        parts = DASH_SPLIT.split(t, maxsplit=1)
        name = (parts[0] if parts else t).strip()
        is_company = ICO_IN_LINE.search(t) is not None

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
            entry = persons.setdefault(name, {"ownership": 0.0, "voting": 0.0, "debug_paths": []})

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
            c.drawImage(img, MARGIN, y_top - target_h, width=target_w, height=target_h,
                        preserveAspectRatio=True, mask='auto')
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

    # Graf
    c.showPage()
    c.setFont(PDF_FONT_NAME, 12)
    c.drawString(MARGIN, PAGE_H - MARGIN - 20, "Grafická struktura")
    c.setFont(PDF_FONT_NAME, 10)
    if graph_png_bytes:
        try:
            img = ImageReader(BytesIO(graph_png_bytes))
            IMG_MAX_W = PAGE_W - 2 * MARGIN
            IMG_MAX_H = PAGE_H - 2 * MARGIN - 60
            c.drawImage(img, MARGIN, MARGIN, width=IMG_MAX_W, height=IMG_MAX_H,
                        preserveAspectRatio=True, anchor='sw', mask='auto')
        except Exception:
            c.drawString(MARGIN, PAGE_H - MARGIN - 40, "⚠️ Nelze vložit obrázek grafu do PDF (chyba při renderu).")
    else:
        c.drawString(MARGIN, PAGE_H - MARGIN - 40,
                     "⚠️ Graf není k dispozici pro PDF. Na Streamlit Cloud je potřeba systémový Graphviz (dot).")

    # Odkazy OR
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

    # UBO vyhodnocení
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

# ===== Export/Import (XML) =====
EXPORT_SCHEMA_VERSION = "2"

def _safe_text(x) -> str:
    return "" if x is None else str(x)

def export_state_to_xml_bytes() -> bytes:
    root = ET.Element("mdg_ubo_export", attrib={"version": EXPORT_SCHEMA_VERSION})
    ET.SubElement(root, "exported_at").text = datetime.now().isoformat(timespec="seconds")

    def add_simple(tag: str, value):
        e = ET.SubElement(root, tag)
        e.text = _safe_text(value)

    add_simple("ico", st.session_state.get("ico_input", ""))
    add_simple("max_depth", st.session_state.get("max_depth", 25))

    # manual_company_owners
    mco = ET.SubElement(root, "manual_company_owners")
    manual_company_owners = st.session_state.get("manual_company_owners", {}) or {}
    for target_ico, owners in manual_company_owners.items():
        comp = ET.SubElement(mco, "company", attrib={"ico": _safe_text(target_ico)})
        for item in owners or []:
            own = ET.SubElement(comp, "owner")
            ET.SubElement(own, "ico").text = _safe_text(item.get("ico"))
            ET.SubElement(own, "share").text = _safe_text(item.get("share"))

    # manual_persons
    mp = ET.SubElement(root, "manual_persons")
    manual_persons = st.session_state.get("manual_persons", {}) or {}
    for name, v in manual_persons.items():
        p = ET.SubElement(mp, "person", attrib={"name": _safe_text(name)})
        for k in ("cap", "vote", "veto", "org_majority", "substitute_ubo"):
            ET.SubElement(p, k).text = _safe_text(v.get(k))

    # overrides
    ov = ET.SubElement(root, "overrides")
    ubo_overrides = st.session_state.get("ubo_overrides", {}) or {}
    ubo_cap_overrides = st.session_state.get("ubo_cap_overrides", {}) or {}
    ov_vote = ET.SubElement(ov, "voting")
    for name, val in ubo_overrides.items():
        it = ET.SubElement(ov_vote, "item", attrib={"name": _safe_text(name)})
        it.text = _safe_text(val)
    ov_cap = ET.SubElement(ov, "capital")
    for name, val in ubo_cap_overrides.items():
        it = ET.SubElement(ov_cap, "item", attrib={"name": _safe_text(name)})
        it.text = _safe_text(val)

    # evaluation settings
    add_simple("threshold_pct_last", st.session_state.get("threshold_pct_last", 25.0))

    vb = ET.SubElement(root, "voting_block")
    ET.SubElement(vb, "block_name").text = _safe_text(st.session_state.get("block_name_last", "Voting Block 1"))
    mem_el = ET.SubElement(vb, "members")
    for n in st.session_state.get("block_members_last", []) or []:
        ET.SubElement(mem_el, "name").text = _safe_text(n)

    # postcheck
    pc = ET.SubElement(root, "postcheck")
    ET.SubElement(pc, "note_text").text = _safe_text(st.session_state.get("note_text", ""))
    ET.SubElement(pc, "check_esm").text = _safe_text(st.session_state.get("check_esm", ""))
    ET.SubElement(pc, "check_structure").text = _safe_text(st.session_state.get("check_structure", ""))
    ET.SubElement(pc, "check_described").text = _safe_text(st.session_state.get("check_described", ""))
    ET.SubElement(pc, "check_fixed").text = _safe_text(st.session_state.get("check_fixed", ""))

    # snapshot vyhodnocení pro okamžité PDF po importu
    snap = ET.SubElement(root, "evaluation_snapshot")
    ubo_lines = (st.session_state.get("last_result") or {}).get("ubo_pdf_lines") or []
    lines_el = ET.SubElement(snap, "ubo_pdf_lines")
    for ln in ubo_lines:
        ET.SubElement(lines_el, "line").text = _safe_text(ln)

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)

def _parse_bool(s: str) -> bool:
    return str(s).strip().lower() in ("1", "true", "yes", "ano")

def import_state_from_xml_bytes(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)

    ico_val = (root.findtext("ico") or "").strip()
    max_depth_val = root.findtext("max_depth") or "25"
    try:
        max_depth_val = int(float(max_depth_val))
    except Exception:
        max_depth_val = 25

    st.session_state["ico_input"] = ico_val
    st.session_state["max_depth"] = max(1, min(60, int(max_depth_val)))

    # manual_company_owners
    mco = {}
    mco_root = root.find("manual_company_owners")
    if mco_root is not None:
        for comp in mco_root.findall("company"):
            tico = (comp.attrib.get("ico") or "").strip()
            owners = []
            for own in comp.findall("owner"):
                oico = (own.findtext("ico") or "").strip()
                share = own.findtext("share") or "0"
                try:
                    share = float(share)
                except Exception:
                    share = 0.0
                if oico:
                    owners.append({"ico": oico, "share": share})
            if tico:
                mco[tico] = owners
    st.session_state["manual_company_owners"] = mco

    # manual_persons
    mp = {}
    mp_root = root.find("manual_persons")
    if mp_root is not None:
        for p in mp_root.findall("person"):
            name = (p.attrib.get("name") or "").strip()
            if not name:
                continue
            def f(tag, default=0.0):
                try:
                    return float(p.findtext(tag) or default)
                except Exception:
                    return default
            mp[name] = {
                "cap": f("cap", 0.0),
                "vote": f("vote", 0.0),
                "veto": _parse_bool(p.findtext("veto") or "false"),
                "org_majority": _parse_bool(p.findtext("org_majority") or "false"),
                "substitute_ubo": _parse_bool(p.findtext("substitute_ubo") or "false"),
            }
    st.session_state["manual_persons"] = mp

    # overrides
    st.session_state["ubo_overrides"] = {}
    st.session_state["ubo_cap_overrides"] = {}

    ov = root.find("overrides")
    if ov is not None:
        ov_vote = ov.find("voting")
        if ov_vote is not None:
            for it in ov_vote.findall("item"):
                name = (it.attrib.get("name") or "").strip()
                try:
                    val = float(it.text or "0")
                except Exception:
                    val = 0.0
                if name:
                    st.session_state["ubo_overrides"][name] = val
        ov_cap = ov.find("capital")
        if ov_cap is not None:
            for it in ov_cap.findall("item"):
                name = (it.attrib.get("name") or "").strip()
                try:
                    val = float(it.text or "0")
                except Exception:
                    val = 0.0
                if name:
                    st.session_state["ubo_cap_overrides"][name] = val

    # evaluation settings
    try:
        st.session_state["threshold_pct_last"] = float(root.findtext("threshold_pct_last") or "25")
    except Exception:
        st.session_state["threshold_pct_last"] = 25.0

    vb = root.find("voting_block")
    if vb is not None:
        st.session_state["block_name_last"] = vb.findtext("block_name") or "Voting Block 1"
        members = []
        mem = vb.find("members")
        if mem is not None:
            for n in mem.findall("name"):
                if n.text and n.text.strip():
                    members.append(n.text.strip())
        st.session_state["block_members_last"] = members

    # postcheck
    pc = root.find("postcheck")
    if pc is not None:
        st.session_state["note_text"] = pc.findtext("note_text") or ""
        st.session_state["check_esm"] = pc.findtext("check_esm") or ""
        st.session_state["check_structure"] = pc.findtext("check_structure") or ""
        st.session_state["check_described"] = pc.findtext("check_described") or ""
        st.session_state["check_fixed"] = pc.findtext("check_fixed") or ""

    # snapshot vyhodnocení
    snap = root.find("evaluation_snapshot")
    imported_ubo_lines = []
    if snap is not None:
        ubo_lines_el = snap.find("ubo_pdf_lines")
        if ubo_lines_el is not None:
            for ln in ubo_lines_el.findall("line"):
                if ln.text is not None:
                    imported_ubo_lines.append(ln.text)

    # uložíme snapshot do session, po resolve ho vložíme do last_result
    st.session_state["imported_ubo_pdf_lines"] = imported_ubo_lines

    # trigger auto resolve
    st.session_state["auto_run_resolve"] = True

# ===== Session state defaults =====
def ss_default(key, val):
    if key not in st.session_state:
        st.session_state[key] = val

ss_default("last_result", None)
ss_default("ubo_overrides", {})
ss_default("ubo_cap_overrides", {})
ss_default("manual_persons", {})
ss_default("final_persons", None)
ss_default("manual_company_owners", {})

ss_default("note_text", "")
ss_default("check_esm", "")
ss_default("check_structure", "")
ss_default("check_described", "")
ss_default("check_fixed", "")

ss_default("threshold_pct_last", 25.0)
ss_default("block_members_last", [])
ss_default("block_name_last", "Voting Block 1")

ss_default("ico_input", "")
ss_default("max_depth", 25)

ss_default("auto_run_resolve", False)

# import control
ss_default("import_uploader_key", 0)
ss_default("import_pending_bytes", None)
ss_default("imported_ubo_pdf_lines", [])

# ===== Header with popovers – vpravo úplně u okraje a těsně vedle sebe =====
HELP_TEXT = """Pro účely zákona o ESM se skutečným majitelem rozumí každá fyzická osoba, která v konečném důsledku vlastní nebo kontroluje právnickou osobu nebo právní uspořádání.

Korporaci v konečném důsledku vlastní nebo kontroluje každá fyzická osoba, která přímo nebo nepřímo prostřednictvím jiné osoby nebo právního uspořádání:
- má podíl v korporaci větší než 25 %
- má podíl na hlasovacích právech větší než 25 %
- má právo na podíl na zisku, jiných zdrojích nebo likvidačním zůstatku větší jak 25 %
- uplatňuje rozhodující vliv v korporaci nebo korporacích, které mají v dané korporaci samostatně nebo společně podíl větší než 25 %
- uplatňuje rozhodující vliv v korporaci jinými prostředky

Rozhodující vliv v:
- korporaci uplatňuje ten, kdo na základě vlastního uvážení, bez ohledu na to, zda a na základě jaké právní skutečnosti, může přímo nebo nepřímo prostřednictvím jiné osoby nebo právního uspořádání dosáhnout toho, že rozhodování nejvyššího orgánu korporace odpovídá jeho vůli. Má se za to, že rozhodující vliv v korporaci uplatňuje ten, kdo může jmenovat nebo odvolat většinu osob, které jsou členy statutárního orgánu korporace.
- obchodní korporaci uplatňuje ovládající osoba podle zákona upravujícího právní poměry obchodních korporací.

Jak na to?
V § 9 odst. 2 písm. b) AML zákona se požaduje, aby povinná osoba v rámci kontroly klienta provedla zjištění totožnosti SM a přijala opatření k ověření jeho totožnosti z důvěryhodných zdrojů s tím, že v případě, že klient podléhá povinnosti zápisu do ESM nebo obdobného registru, povinná osoba ověřila SM vždy alespoň z této ESM nebo obdobného registru a z jednoho dalšího zdroje.

Základním východiskem povinnosti zjišťování SM v AML zákoně je získání údajů z ESM, či jiné obdobné evidence. Vedle toho je požadováno, aby tento základní zdroj, jejž lze beze sporu považovat za zdroj vysoce důvěryhodný, byl doplněn dalším zdrojem.

Povinná osoba by měla vycházet z kategorizace klientů podle rizikového profilu, kdy u klientů s nízkým či středním rizikem ML/TF se lze spokojit s doplňujícím zdrojem v podobě dokumentovaného čestného prohlášení SM nebo člena statutárního orgánu klienta - právnické osoby. Zatímco v případě vysoce rizikových klientů je nutné informace z ESM doplnit o vlastní šetření povinné osoby.

Jsou-li naplněny zákonné podmínky pro povinné provedení zesílené kontroly, pak z § 9a odst. 3 písm. a) bodu 1 AML zákona vyplývá povinnost získat další informace nebo dokumenty o SM.

Aktualizaci informací o SM lze provést nahlédnutím do ESM, případně také dotazem na klienta. Případný dotaz na klienta musí být zaznamenán a formulován tak, aby klient odpověděl komisivně, čili aby výslovně odsouhlasil aktuálnost údajů v ESM. Pokud klient neposkytne potřebnou součinnost, je na místě uplatnit postup podle § 15 AML zákona.
"""

h_left, h_right = st.columns([8.5, 1.5], vertical_alignment="top")
with h_left:
    # logo + nadpis v jedné řádce, stejné levé zarovnání, malá mezera
    c_logo, c_title = st.columns([1.2, 8.8], vertical_alignment="center")
    with c_logo:
        if logo_bytes:
            st.image(logo_bytes, width=480)  # velké ale rozumné
    with c_title:
        st.markdown("## MDG UBO Tool - AML kontrola vlastnické struktury na ARES")
        st.markdown(
            '<div class="small-muted">Online režim: společníci/akcionáři se načítají z ARES VR API (např. /ekonomicke-subjekty-vr/{ICO}).</div>',
            unsafe_allow_html=True
        )


with h_right:
    # na úplný pravý okraj, popovery vedle sebe
    r1, r2 = st.columns([1.2, 0.7], vertical_alignment="top")
    with r1:
        with st.popover("UŽITEČNÉ ODKAZY"):
            st.markdown(
                "- Zákon - ESM: https://www.zakonyprolidi.cz/cs/2021-37\n"
                "- Zákon - AML: https://www.zakonyprolidi.cz/cs/2008-253\n"
                "- Metodické pokyny FAÚ: https://www.fau.gov.cz/cs/rozcestnik/legislativa-a-metodika/metodicke-pokyny-265\n"
                "- Příručka evidování skutečných majitelů: https://www.fau.gov.cz/assets/cs/cmsmedia/legislativa-a-metodika/prirucka-evidovani-skutecnych-majitelu-d.pdf"
            )
    with r2:
        with st.popover("❓"):
            st.markdown(HELP_TEXT)

st.markdown("<br>", unsafe_allow_html=True)

# ===== UI vstupy + EXPORT/IMPORT na úrovni tlačítka =====
ico = st.text_input("IČO společnosti", value=st.session_state.get("ico_input", ""), placeholder="např. 03999840")
st.session_state["ico_input"] = ico

max_depth = st.slider("Max. hloubka rozkrytí", 1, 60, int(st.session_state.get("max_depth", 25)), 1)
st.session_state["max_depth"] = int(max_depth)

top1, top2, top3, top4 = st.columns([1.35, 1.05, 1.15, 3.0], vertical_alignment="center")
with top1:
    run = st.button("🔎 Rozkrýt strukturu", type="primary")

with top2:
    xml_bytes = export_state_to_xml_bytes()
    st.download_button(
        label="⬇️ EXPORT (XML)",
        data=xml_bytes,
        file_name=f"mdg_ubo_export_{(ico.strip() or 'noico')}.xml",
        mime="application/xml",
        use_container_width=True,
    )

with top3:
    with st.popover("⬆️ IMPORT (XML)"):
        up = st.file_uploader(
            "Vyber XML soubor exportovaný z této aplikace",
            type=["xml"],
            key=f"import_uploader_{st.session_state['import_uploader_key']}",
        )
        if up is not None:
            st.session_state["import_pending_bytes"] = up.read()
            st.info("Soubor nahrán. Klikni na **Načíst import**.")
        if st.button("Načíst import", type="primary"):
            if not st.session_state.get("import_pending_bytes"):
                st.warning("Nejprve nahraj XML soubor.")
            else:
                try:
                    import_state_from_xml_bytes(st.session_state["import_pending_bytes"])
                    st.session_state["import_pending_bytes"] = None
                    st.session_state["import_uploader_key"] += 1  # reset uploaderu = konec loopu
                    st.success("Import hotový. Obnovuji stav a znovu načtu strukturu…")
                    st.rerun()
                except Exception as e:
                    st.error(f"Import selhal: {e}")

with top4:
    st.write("")

# ===== Resolve logic =====
def do_resolve():
    if not ico.strip():
        st.error("Zadej IČO.")
        return

    cb = progress_ui(); cb("Start…", 0.01)
    try:
        client = AresVrClient(ares_db_path)
        cb("Načítám z ARES a rozkrývám…", 0.10)

        manual_overrides = {
            k: [(item["ico"], item["share"]) for item in v]
            for k, v in (st.session_state.get("manual_company_owners") or {}).items()
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

        g = build_graphviz_from_nodelines_bfs(
            lines,
            root_ico=ico.strip(),
            title=f"Ownership_{ico.strip()}",
        )

        graph_png = None
        try:
            graph_png = g.pipe(format="png")  # vyžaduje systémový graphviz
        except Exception:
            graph_png = None

        companies = extract_companies_from_lines(lines)

        st.session_state["last_result"] = {
            "lines": lines,
            "warnings": warnings,
            "graphviz": g,
            "graph_png": graph_png,
            "text_lines": rendered,
            "companies": companies,
            "ubo_pdf_lines": (st.session_state.get("last_result") or {}).get("ubo_pdf_lines"),
            "unresolved": [w for w in warnings if isinstance(w, dict) and w.get("kind") == "unresolved"],
        }

        # po importu: pokud máme snapshot ubo_pdf_lines, přeneseme ho do last_result
        imported_lines = st.session_state.get("imported_ubo_pdf_lines") or []
        if imported_lines:
            st.session_state["last_result"]["ubo_pdf_lines"] = imported_lines

        st.success("Struktura byla načtena. Níže se zobrazí výsledky.")
    except Exception as e:
        st.error("Spadlo to na chybě:")
        st.code(str(e))

# Button / auto-run
if run:
    # při ručním runu nemažeme importované volby, jen znovu načteme strukturu
    do_resolve()

if st.session_state.get("auto_run_resolve"):
    st.session_state["auto_run_resolve"] = False
    if ico.strip():
        do_resolve()

# ===== Persistentní render =====
lr = st.session_state.get("last_result")
if lr:
    st.subheader("Výsledek (Text)")
    st.caption("Odsazení = úroveň. Každý blok: firma → její společníci/akcionáři.")
    st.code("\n".join(lr["text_lines"]), language="text")

    st.subheader("Výsledek (Graf)")
    try:
        st.graphviz_chart(lr["graphviz"].source)
        if lr.get("graph_png") is None:
            st.warning(
                "Graf se zobrazuje v aplikaci, ale do PDF se nevloží obrázek (chybí Graphviz 'dot' pro render PNG). "
                "Na Streamlit Cloud přidej do repa `packages.txt` s řádkem: `graphviz`."
            )
    except Exception:
        st.warning("Nelze zobrazit graf (Graphviz).")

    # ===== Manuální doplnění vlastníků =====
    st.subheader("Doplnění vlastníků u firem bez dohledaných společníků/akcionářů")
    st.caption("Vyber firmu bez vlastníků (OR) a doplň její vlastníky (IČO + podíl). Po přidání se struktura rekurzivně rozbalí až k FO.")

    unresolved_list = lr.get("unresolved") or []
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

                st.session_state["manual_company_owners"][target_ico] = parsed
                do_resolve()
                st.success(f"Přidáno: {target_name} (IČO {target_ico}) — vlastníci doplněni, struktura znovu rozkryta.")
                st.rerun()

    # ===== OR links + PDF without UBO =====
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

    # ===== SKUTEČNÍ MAJITELÉ =====
    st.subheader("SKUTEČNÍ MAJITELÉ (dle OR)")
    st.caption("Automatický přepočet textových podílů, násobení napříč patry. Úpravy ZK/HP v %, právo veta, „jmenuje/odvolává většinu orgánu“, náhradní SM (§ 5 ZESM) a voting block. Práh je striktně > nastavené hodnoty.")

    persons = compute_effective_persons(lr["lines"])

    show_debug = st.checkbox("Zobrazit diagnostiku výpočtu (cesty a násobení)", value=False)
    if show_debug:
        st.info("Diagnostika: pro každou osobu jsou uvedeny cesty s multiplikátorem rodiče, lokálním podílem a efektivním příspěvkem.")
        for name, info in persons.items():
            st.markdown(f"**{name}** — efektivní kapitál: {fmt_pct(info['ownership'])}, hlasovací práva: {fmt_pct(info['voting'])}")
            for i, dp in enumerate(info.get("debug_paths", []), 1):
                def _fmt(x):
                    return "—" if x is None else f"{x*100.0:.2f}%"
                st.markdown(
                    f"- cesta {i}: multiplikátor rodiče **{_fmt(dp.get('parent_mult'))}**, "
                    f"lokální podíl **{_fmt(dp.get('local_share'))}**, efektivní **{_fmt(dp.get('eff'))}**; zdroj `{dp.get('source')}`\n"
                    f"  \n  ↳ `{dp.get('text')}`"
                )
            st.markdown("---")

    # Manuální osoby
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
        manual_substitute_ubo = st.checkbox("Náhradní skutečný majitel (§ 5 ZESM)", value=False, key="manual_substitute_ubo")
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
        st.success(f"Přidáno: {manual_name.strip()}")

    if st.session_state["manual_persons"]:
        st.markdown("**Manuálně přidané osoby:**")
        for mn, mi in list(st.session_state["manual_persons"].items()):
            colR1, colR2 = st.columns([6, 1])
            with colR1:
                st.markdown(
                    f"- **{mn}** — kapitál: {fmt_pct(mi['cap'])}, hlasovací: {fmt_pct(mi['vote'])}, "
                    f"veto: {'ano' if mi['veto'] else 'ne'}, org.: {'ano' if mi['org_majority'] else 'ne'}, "
                    f"náhradní SM: {'ano' if mi.get('substitute_ubo') else 'ne'}"
                )
            with colR2:
                if st.button(f"🗑️ Odebrat ({mn})", key=f"del_{mn}"):
                    st.session_state["manual_persons"].pop(mn, None)
                    st.rerun()

    overrides_vote = st.session_state["ubo_overrides"]
    overrides_cap = st.session_state["ubo_cap_overrides"]

    with st.form("ubo_form"):
        threshold_pct = st.number_input(
            "Práh pro skutečného majitele (%)",
            min_value=0.0, max_value=100.0, value=float(st.session_state.get("threshold_pct_last", 25.0)), step=0.01,
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
                st.markdown(f"  • Kapitál: **{fmt_pct(info['ownership'])}**")
                st.markdown(f"  • Hlasovací: **{fmt_pct(info['voting'])}**")
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
                veto_flags[name] = st.checkbox(f"Právo veta ({name})", value=False, key=f"veto_{idx}_{name}")
                org_majority_flags[name] = st.checkbox(f"Jmenuje/odvolává většinu orgánu ({name})", value=False, key=f"orgmaj_{idx}_{name}")
            with colE:
                substitute_flags[name] = st.checkbox(
                    f"Náhradní SM (§ 5) ({name})",
                    value=False,
                    key=f"subs_{idx}_{name}",
                )

        st.divider()
        st.write("**Jednání ve shodě (voting block):**")
        all_names = list(set(list(persons.keys()) + list(st.session_state["manual_persons"].keys())))
        block_members = st.multiselect(
            "Vyber účastníky voting blocku",
            all_names,
            st.session_state.get("block_members_last", []),
        )
        block_name = st.text_input("Název voting blocku", value=st.session_state.get("block_name_last", "Voting Block 1"))

        submitted = st.form_submit_button("Vyhodnotit skutečné majitele")

    if submitted:
        st.session_state["threshold_pct_last"] = float(threshold_pct)
        st.session_state["block_members_last"] = list(block_members)
        st.session_state["block_name_last"] = str(block_name)

        for n, v in edited_voting_pct.items():
            overrides_vote[n] = v / 100.0
        for n, v in edited_cap_pct.items():
            overrides_cap[n] = v / 100.0

        final_persons: dict[str, dict] = {}
        for n, info in persons.items():
            final_persons[n] = {
                "cap": overrides_cap.get(n, info["ownership"]),
                "vote": overrides_vote.get(n, info["voting"]),
                "veto": veto_flags.get(n, False),
                "org_majority": org_majority_flags.get(n, False),
                "substitute_ubo": substitute_flags.get(n, False),
            }
        for mn, mi in st.session_state["manual_persons"].items():
            final_persons[mn] = {
                "cap": mi["cap"],
                "vote": mi["vote"],
                "veto": mi["veto"],
                "org_majority": mi["org_majority"],
                "substitute_ubo": mi.get("substitute_ubo", False),
            }
        st.session_state["final_persons"] = final_persons

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

        block_total = sum(final_persons.get(n, {"vote": 0.0})["vote"] for n in block_members) if block_members else 0.0

        thr = (threshold_pct / 100.0)
        ubo: dict[str, dict] = {}
        reasons: dict[str, list[str]] = {}
        def add_reason(n: str, r: str):
            reasons.setdefault(n, []).append(r)

        for n, vals in final_persons.items():
            cap = vals["cap"]; vote = vals["vote"]
            veto = vals.get("veto", False)
            orgmaj = vals.get("org_majority", False)
            substitute = vals.get("substitute_ubo", False)
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
                is_ubo = True; add_reason(n, "náhradní skutečný majitel (§ 5 ZESM)")
            if is_ubo:
                ubo[n] = vals

        if block_members and block_total > thr:
            for n in block_members:
                if n in final_persons:
                    ubo[n] = final_persons[n]
                    add_reason(n, f"účast ve voting blocku „{block_name}“ s {fmt_pct(block_total)} > {threshold_pct:.2f}%")

        st.success("Vyhodnocení dokončeno.")
        ubo_report_lines = []
        ubo_report_lines.append(f"Součet podílů na ZK: {total_cap*100.0:.2f}% ({'OK' if cap_ok else '⚠︎'})")
        ubo_report_lines.append(f"Součet hlasovacích práv: {total_vote*100.0:.2f}% ({'OK' if vote_ok else '⚠︎'})")

        if not ubo:
            st.info("Nebyly zjištěny fyzické osoby splňující definici skutečného majitele dle zadaných pravidel.")
        else:
            st.markdown("**Skuteční majitelé:**")
            for n, vals in ubo.items():
                rs = "; ".join(reasons.get(n, []))
                line_txt = f"- {n} — kapitál: {fmt_pct(vals['cap'])}, hlasovací práva: {fmt_pct(vals['vote'])} — {rs}"
                st.markdown(line_txt)
                ubo_report_lines.append(line_txt)

        st.session_state["last_result"]["ubo_pdf_lines"] = ubo_report_lines
        st.session_state["imported_ubo_pdf_lines"] = ubo_report_lines  # aby export/import byl konzistentní

    # ===== POST-CHECK + PDF (po vyhodnocení nebo po importu snapshotu) =====
    if lr.get("ubo_pdf_lines"):
        st.divider()
        st.markdown("### Poznámka a kontrolní otázky")

        st.session_state["note_text"] = st.text_area(
            "Poznámka",
            value=st.session_state.get("note_text", ""),
            placeholder="Sem může uživatel napsat cokoliv…",
            height=120,
        )

        c1, c2 = st.columns([1, 1])
        with c1:
            st.session_state["check_esm"] = st.radio(
                "Zápis v evidenci skutečných majitelů:",
                options=["", "✅ souhlasí", "❌ nesouhlasí"],
                index=["", "✅ souhlasí", "❌ nesouhlasí"].index(st.session_state.get("check_esm", "") or ""),
                horizontal=True,
            )
        with c2:
            st.session_state["check_structure"] = st.radio(
                "Struktura vztahů:",
                options=["", "✅ souhlasí", "❌ nesouhlasí"],
                index=["", "✅ souhlasí", "❌ nesouhlasí"].index(st.session_state.get("check_structure", "") or ""),
                horizontal=True,
            )

        any_negative = (st.session_state.get("check_esm") == "❌ nesouhlasí") or (st.session_state.get("check_structure") == "❌ nesouhlasí")
        if any_negative:
            d1, d2 = st.columns([1, 1])
            with d1:
                st.session_state["check_described"] = st.radio(
                    "Byla nesrovnalost popsána?",
                    options=["", "✅ ano", "❌ ne"],
                    index=["", "✅ ano", "❌ ne"].index(st.session_state.get("check_described", "") or ""),
                    horizontal=True,
                )
            with d2:
                st.session_state["check_fixed"] = st.radio(
                    "Byla nesrovnalost napravena?",
                    options=["", "✅ ano", "❌ ne"],
                    index=["", "✅ ano", "❌ ne"].index(st.session_state.get("check_fixed", "") or ""),
                    horizontal=True,
                )
        else:
            st.session_state["check_described"] = ""
            st.session_state["check_fixed"] = ""

        ubo_lines_for_pdf = list(lr["ubo_pdf_lines"])
        ubo_lines_for_pdf.append("")
        ubo_lines_for_pdf.append("Poznámka:")
        ubo_lines_for_pdf.append(st.session_state.get("note_text", "") or "—")
        ubo_lines_for_pdf.append("")
        ubo_lines_for_pdf.append(f"Zápis v evidenci skutečných majitelů: {st.session_state.get('check_esm') or '—'}")
        ubo_lines_for_pdf.append(f"Struktura vztahů: {st.session_state.get('check_structure') or '—'}")
        if any_negative:
            ubo_lines_for_pdf.append(f"Byla nesrovnalost popsána: {st.session_state.get('check_described') or '—'}")
            ubo_lines_for_pdf.append(f"Byla nesrovnalost napravena: {st.session_state.get('check_fixed') or '—'}")

        pdf_bytes_with_ubo = build_pdf(
            text_lines=lr["text_lines"],
            graph_png_bytes=lr["graph_png"],
            logo_bytes=logo_bytes,
            company_links=company_links_now,
            ubo_lines=ubo_lines_for_pdf,
        )
        st.download_button(
            label="📄 Generovat do PDF (včetně vyhodnocení SM a součtů)",
            data=pdf_bytes_with_ubo,
            file_name=f"ownership_ubo_{ico.strip() or 'export'}.pdf",
            mime="application/pdf",
            type="primary",
        )

    # ===== Upozornění =====
    if lr.get("warnings") or lr.get("unresolved"):
        st.subheader("Upozornění")
        for w in lr.get("warnings", []):
            if hasattr(w, "text"):
                st.warning(str(getattr(w, "text", w)))
            elif isinstance(w, dict):
                st.warning(str(w.get("text", w)))
            else:
                st.warning(str(w))
