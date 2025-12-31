# importer/graphviz_render.py
from __future__ import annotations

import re
import hashlib
import html
from typing import Any, Dict, List, Optional, Tuple

from graphviz import Digraph


# Firma header: "Název (IČO 12345678)"
RE_COMPANY_HEADER = re.compile(r"^(?P<name>.+)\s+\(IČO\s+(?P<ico>\d{8})\)\s*$")

# Foreign header: "Název (ID Z45156824)"
RE_FOREIGN_HEADER = re.compile(r"^(?P<name>.+)\s+\(ID\s+(?P<fid>[A-Za-z0-9]+)\)\s*$")

# Robustní detekce IČO uvnitř řádku (firma vlastník)
ICO_IN_LINE = re.compile(r"\(IČO\s+(?P<ico>\d{7,8})\)")

# Robustní detekce ID uvnitř řádku (foreign vlastník)
ID_IN_LINE = re.compile(r"\(ID\s+(?P<fid>[A-Za-z0-9]+)\)")

# Rozdělení jméno/podíl podle jakékoliv pomlčky s mezerami kolem
DASH_SPLIT = re.compile(r"\s+[—–-]\s+")


def _ensure_list(x):
    if x is None:
        return []
    if isinstance(x, (list, tuple)):
        return list(x)
    return [x]


def _get_depth_text(ln: Any) -> Tuple[int, str]:
    if hasattr(ln, "text"):
        d = getattr(ln, "depth", 0) or 0
        t = getattr(ln, "text", "")
        return int(d), str(t)

    if isinstance(ln, dict):
        d = ln.get("depth", 0) or 0
        t = ln.get("text", "")
        return int(d), str(t)

    if isinstance(ln, (tuple, list)) and len(ln) >= 2:
        return int(ln[0] or 0), str(ln[1])

    return 0, str(ln)


def _norm_ico(ico: str) -> str:
    digits = re.sub(r"\D+", "", ico or "")
    if len(digits) == 7:
        digits = "0" + digits
    return digits.zfill(8)


def _norm_fid(fid: str) -> str:
    return (fid or "").strip().upper()


def _node_id(prefix: str, text: str) -> str:
    h = hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{h}"


def build_graphviz_from_nodelines_bfs(
    lines: List[Any],
    root_ico: str,
    title: str = "Ownership",
) -> Digraph:
    """
    Patrové zobrazení (BFS):
    1) root firma
    2) její vlastníci
    3) vlastníci vlastníků (u firem/foreign)
    atd.

    Opravy:
    - zahraniční uzly (ID) jsou plnohodnotné uzly ve stacku => FO se napojí správně na foreign
    - foreign owner řádek + foreign header řádek se sloučí do 1 uzlu (ID-based node_id)
    - foreign uzly jsou boxy + jiná barva
    - deduplikace hran a doplňování labelů zachováno
    """

    root_ico = _norm_ico(root_ico)
    items = _ensure_list(lines)

    g = Digraph(name="ownership", format="png")
    g.attr(label=title, labelloc="t", fontsize="20")
    g.attr(rankdir="TB")  # shora dolů
    g.attr(splines="true")
    g.attr(overlap="false")
    g.attr(fontname="Helvetica")

    # ---------- Global styling ----------
    g.attr("edge", dir="back", color="gray40", fontname="Helvetica", fontsize="10", fontcolor="black")
    g.attr("node", fontcolor="white", fontname="Helvetica", fontsize="10", margin="0.05,0.04")
    g.attr(ranksep="0.7", nodesep="0.35")

    # Barvy
    COMPANY_FILL = "#2EA39C"   # RGB(46,163,156)
    FOREIGN_FILL = "#E67E22"   # kontrastní k #2EA39C
    PERSON_FILL = "#000000"    # black

    # Osoby: vodorovné elipsy s levým zarovnáním a zalomením
    PERSON_WIDTH = 2.0         # pevná šířka (palce)
    BASE_PERSON_HEIGHT = 0.80  # minimální výška
    LINE_HEIGHT_IN = 0.18      # výška řádku (~10pt)
    WRAP_MAX_CHARS = 22        # cca znaků na řádek

    # stack aktuální entity podle hloubky:
    # depth -> (etype, eid, name, level)
    # etype: "CZ" | "FOREIGN"
    entity_stack: Dict[int, Tuple[str, str, str, int]] = {}

    # rank buckets: level -> [node_id...]
    ranks: Dict[int, List[str]] = {}

    # aktuální level uzlu (kvůli přesunu do hlubšího ranku)
    node_level: Dict[str, int] = {}

    # evidence/atributy hran – vykreslí se až na konci
    edge_attrs: Dict[Tuple[str, str], Dict[str, str]] = {}

    def record_edge(u: str, v: str, label: Optional[str] = None):
        """Eviduj hranu u->v; label doplň/nahraď pokud přichází později."""
        if not u or not v or u == v:
            return
        key = (u, v)
        attrs = edge_attrs.get(key, {})
        if label is not None and label.strip():
            attrs["label"] = label.strip()
        edge_attrs[key] = attrs

    def entity_level_from_depth(d: int) -> int:
        if d <= 0:
            return 0
        return d // 3

    def add_to_rank(level: int, nid: str):
        prev = node_level.get(nid)
        if prev is None:
            node_level[nid] = level
            ranks.setdefault(level, [])
            if nid not in ranks[level]:
                ranks[level].append(nid)
            return
        if level <= prev:
            return
        node_level[nid] = level
        if prev in ranks and nid in ranks[prev]:
            ranks[prev].remove(nid)
        ranks.setdefault(level, [])
        if nid not in ranks[level]:
            ranks[level].append(nid)

    def add_company_node(ico: str, name: str, level: int) -> str:
        ico = _norm_ico(ico)
        nid = f"ICO_{ico}"
        g.node(
            nid,
            f"{name}\n(IČO {ico})",
            shape="box",
            style="filled",
            fillcolor=COMPANY_FILL,
            color=COMPANY_FILL,
        )
        add_to_rank(level, nid)
        return nid

    def add_foreign_node(fid: str, name: str, level: int) -> str:
        fid = _norm_fid(fid)
        nid = f"FID_{fid}"
        g.node(
            nid,
            f"{name}\n(ID {fid})",
            shape="box",
            style="filled",
            fillcolor=FOREIGN_FILL,
            color=FOREIGN_FILL,
        )
        add_to_rank(level, nid)
        return nid

    # ---------- Label helper: zalomení a levé zarovnání ----------
    def _wrap_text(text: str, max_chars: int = 22) -> List[str]:
        text = (text or "").strip()
        if not text:
            return []
        words = text.split()
        lines: List[str] = []
        cur: List[str] = []
        cur_len = 0
        for w in words:
            wlen = len(w)
            if cur_len == 0:
                cur.append(w)
                cur_len = wlen
            else:
                if cur_len + 1 + wlen <= max_chars:
                    cur.append(w)
                    cur_len += 1 + wlen
                else:
                    lines.append(" ".join(cur))
                    cur = [w]
                    cur_len = wlen
        if cur:
            lines.append(" ".join(cur))
        return lines

    def _html_label_left_wrapped(text: str, max_chars: int = 22, point_size: int = 10) -> str:
        lines = _wrap_text(text, max_chars=max_chars)
        safe_lines = [html.escape(line, quote=True) for line in lines] or [""]
        rows = "\n".join(
            f'  <TR><TD ALIGN="LEFT"><FONT FACE="Helvetica" POINT-SIZE="{point_size}" COLOR="white">{ln}</FONT></TD></TR>'
            for ln in safe_lines
        )
        return f"""<
<TABLE BORDER="0" CELLBORDER="0" CELLPADDING="0" CELLSPACING="0">
{rows}
</TABLE>
>"""

    def add_person_node(label: str, level: int, unique_key: str) -> str:
        wrapped_lines = _wrap_text(label, max_chars=WRAP_MAX_CHARS)
        n_lines = max(1, len(wrapped_lines))
        dynamic_height = BASE_PERSON_HEIGHT + (n_lines - 1) * LINE_HEIGHT_IN

        nid = _node_id("P", unique_key)
        g.node(
            nid,
            _html_label_left_wrapped(label, max_chars=WRAP_MAX_CHARS, point_size=10),
            shape="ellipse",
            style="filled",
            fillcolor=PERSON_FILL,
            color=PERSON_FILL,
            fixedsize="true",
            width=str(PERSON_WIDTH),
            height=str(dynamic_height),
            penwidth="1",
        )
        add_to_rank(level, nid)
        return nid

    # ---------- Parsování owner řádků ----------
    def parse_company_owner_line(t: str) -> Optional[Tuple[str, str, str]]:
        tm = ICO_IN_LINE.search(t)
        if not tm:
            return None
        owner_ico = _norm_ico(tm.group("ico"))
        left = (t[:tm.start()] or "").strip()
        parts = DASH_SPLIT.split(left, maxsplit=1)
        if len(parts) == 2:
            owner_name = parts[0].strip()
            share_text = parts[1].strip()
        else:
            return None
        return owner_name, share_text, owner_ico

    def parse_foreign_owner_line(t: str) -> Optional[Tuple[str, str, str]]:
        tm = ID_IN_LINE.search(t)
        if not tm:
            return None
        fid = _norm_fid(tm.group("fid"))
        left = (t[:tm.start()] or "").strip()
        parts = DASH_SPLIT.split(left, maxsplit=1)
        if len(parts) == 2:
            owner_name = parts[0].strip()
            share_text = parts[1].strip()
        else:
            return None
        return owner_name, share_text, fid

    def parse_person_owner_line(t: str) -> Optional[Tuple[str, str]]:
        parts = DASH_SPLIT.split(t.strip(), maxsplit=1)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
        return None

    def find_parent_entity(depth: int) -> Optional[Tuple[str, str, str, int]]:
        candidates = [(d, v) for d, v in entity_stack.items() if d < depth]
        if not candidates:
            return None
        _, v = max(candidates, key=lambda x: x[0])
        return v

    def entity_node_id(etype: str, eid: str) -> str:
        if etype == "CZ":
            return f"ICO_{_norm_ico(eid)}"
        return f"FID_{_norm_fid(eid)}"

    # ---------- Parsování vstupu a evidence hran ----------
    for idx, ln in enumerate(items):
        depth, text = _get_depth_text(ln)
        t = (text or "").strip()
        if not t:
            continue

        # Strukturální labely ignorujeme jako samostatné uzly v grafu
        if t.endswith(":"):
            hdr = t[:-1].strip().lower()
            if hdr in ("společníci", "akcionáři", "manuálně doplněno"):
                continue

        # 1) owner řádek CZ firmy
        parsed_company = parse_company_owner_line(t)
        if parsed_company:
            owner_name, share_text, owner_ico = parsed_company
            parent = find_parent_entity(depth)
            if parent is None:
                continue
            p_type, p_id, _, p_level = parent
            parent_id = entity_node_id(p_type, p_id)

            owner_id = add_company_node(owner_ico, owner_name, p_level + 1)
            record_edge(parent_id, owner_id, label=share_text)
            continue

        # 2) owner řádek FOREIGN subjektu
        parsed_foreign = parse_foreign_owner_line(t)
        if parsed_foreign:
            owner_name, share_text, fid = parsed_foreign
            parent = find_parent_entity(depth)
            if parent is None:
                continue
            p_type, p_id, _, p_level = parent
            parent_id = entity_node_id(p_type, p_id)

            foreign_id = add_foreign_node(fid, owner_name, p_level + 1)
            record_edge(parent_id, foreign_id, label=share_text)
            continue

        # 3) header CZ firmy
        m = RE_COMPANY_HEADER.match(t)
        if m:
            ico = _norm_ico(m.group("ico"))
            name = m.group("name").strip()
            level = entity_level_from_depth(depth)

            child_id = add_company_node(ico, name, level)

            parent = find_parent_entity(depth)
            if parent is not None:
                p_type, p_id, _, _p_level = parent
                parent_id = entity_node_id(p_type, p_id)
                record_edge(parent_id, child_id)

            entity_stack[depth] = ("CZ", ico, name, level)
            for d in list(entity_stack.keys()):
                if d > depth:
                    del entity_stack[d]
            continue

        # 4) header FOREIGN subjektu
        mf = RE_FOREIGN_HEADER.match(t)
        if mf:
            fid = _norm_fid(mf.group("fid"))
            name = mf.group("name").strip()
            level = entity_level_from_depth(depth)

            child_id = add_foreign_node(fid, name, level)

            parent = find_parent_entity(depth)
            if parent is not None:
                p_type, p_id, _, _p_level = parent
                parent_id = entity_node_id(p_type, p_id)
                record_edge(parent_id, child_id)

            entity_stack[depth] = ("FOREIGN", fid, name, level)
            for d in list(entity_stack.keys()):
                if d > depth:
                    del entity_stack[d]
            continue

        # 5) osoba (vlastník)
        parent = find_parent_entity(depth)
        if parent is None:
            continue
        p_type, p_id, _, p_level = parent
        parent_id = entity_node_id(p_type, p_id)

        person_id = add_person_node(t, p_level + 1, unique_key=f"{p_type}:{p_id}:{idx}:{t}")

        parsed_person = parse_person_owner_line(t)
        if parsed_person:
            _, share_text = parsed_person
            record_edge(parent_id, person_id, label=share_text)
        else:
            record_edge(parent_id, person_id)

    # ---------- Vykreslení hran až teď (s doplněnými labely) ----------
    for (u, v), attrs in edge_attrs.items():
        if u == v:
            continue
        g.edge(u, v, **attrs)

    # rank=same pro patra
    for level, nodes in ranks.items():
        if not nodes:
            continue
        with g.subgraph(name=f"rank_{level}") as sg:
            sg.attr(rank="same")
            for nid in nodes:
                sg.node(nid)

    return g
