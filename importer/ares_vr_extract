
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import re
from datetime import datetime


# === Pomocné: výběr primárního záznamu ===
def _pick_primary_or_record(zaznamy: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not zaznamy:
        return None
    prim = [z for z in zaznamy if z.get("primarniZaznam") is True]
    return prim[0] if prim else zaznamy[0]


# === Aktivní položka (bez datumVymazu) ===
def _is_active_item(item: Dict[str, Any]) -> bool:
    return not item.get("datumVymazu")


# === Normalizace IČO na 8 míst ===
def _normalize_ico(ico: Optional[str]) -> Optional[str]:
    if not ico:
        return None
    ico = re.sub(r"\D", "", ico.strip())
    return ico.zfill(8) if ico else None


# === Parsování datumu ===
def _parse_date(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


# === Robustní parser podílů z TEXTU (vrací procenta 0..100) ===
PCT_RE = re.compile(r"(\d+(?:[.,;]\d+)?)\s*%")
PROCENTA_RE = re.compile(r"(\d+(?:[.,;]\d+)?)\s*PROCENTA", re.IGNORECASE)
FRAC_SLASH_RE = re.compile(r"(\d+)\s*/\s*(\d+)")
FRAC_SEMI_RE = re.compile(r"(\d+)\s*;\s*(\d+)\s*(ZLOMEK|TEXT)?", re.IGNORECASE)

OBCHODNI_PODIL_FRAC_RE = re.compile(r"obchodni[_ ]?podil\s*:\s*(\d+)\s*[/;]\s*(\d+)", re.IGNORECASE)
OBCHODNI_PODIL_PCT_RE  = re.compile(r"obchodni[_ ]?podil\s*:\s*(\d+(?:[.,;]\d+)?)\s*(?:%|PROCENTA)", re.IGNORECASE)

HLASOVACI_PRAVA_PCT_RE = re.compile(r"hlasovaci[_ ]?prava\s*:\s*(\d+(?:[.,;]\d+)?)\s*(?:%|PROCENTA)", re.IGNORECASE)
SPLACENO_FIELD_RE      = re.compile(r"splaceno\s*:\s*\d+(?:[.,;]\d+)?\s*PROCENTA", re.IGNORECASE)

def _to_float(s: str) -> Optional[float]:
    try:
        return float(s.replace(",", ".").replace(";", "."))
    except Exception:
        return None

def _parse_pct_from_text(s: str) -> Optional[float]:
    """
    Přetaví text OR na podíl v PROCENTECH (0..100).
    1) sečte VŠECHNY výskyty 'obchodni_podil' (zlomky i %), ignoruje 'splaceno:… PROCENTA'
    2) pokud chybí, sečte VŠECHNY 'hlasovaci_prava' (%)
    3) pak obecné zlomky ('a/b', 'a;b') – všechny výskyty
    4) nakonec obecné procenta ('X %' / 'X PROCENTA') – všechny výskyty
    Výsledek zastropuje na 100.0. Vrací None, pokud nic nenajde.
    """
    s = (s or "").strip()
    if not s:
        return None

    # ignoruj 'splaceno:... PROCENTA'
    s = SPLACENO_FIELD_RE.sub("", s)

    # 1) obchodni_podil – zlomek + %
    total = 0.0
    found = False
    for m in OBCHODNI_PODIL_FRAC_RE.finditer(s):
        a = _to_float(m.group(1)); b = _to_float(m.group(2))
        if a is not None and b and b != 0:
            total += (a / b) * 100.0; found = True
    for m in OBCHODNI_PODIL_PCT_RE.finditer(s):
        v = _to_float(m.group(1))
        if v is not None:
            total += v; found = True
    if found:
        return max(0.0, min(100.0, total))

    # 2) hlasovaci_prava – % (všechny)
    hv_total = 0.0; hv_found = False
    for m in HLASOVACI_PRAVA_PCT_RE.finditer(s):
        v = _to_float(m.group(1))
        if v is not None:
            hv_total += v; hv_found = True
    if hv_found:
        return max(0.0, min(100.0, hv_total))

    # 3) obecné zlomky – a/b, a;b
    frac_total = 0.0; frac_found = False
    for m in FRAC_SLASH_RE.finditer(s):
        a = _to_float(m.group(1)); b = _to_float(m.group(2))
        if a is not None and b and b != 0:
            frac_total += (a / b) * 100.0; frac_found = True
    for m in FRAC_SEMI_RE.finditer(s):
        a = _to_float(m.group(1)); b = _to_float(m.group(2))
        if a is not None and b and b != 0:
            frac_total += (a / b) * 100.0; frac_found = True
    if frac_found:
        return max(0.0, min(100.0, frac_total))

    # 4) obecná procenta – X%, X PROCENTA
    pct_total = 0.0; pct_found = False
    for m in PCT_RE.finditer(s):
        v = _to_float(m.group(1))
        if v is not None:
            pct_total += v; pct_found = True
    for m in PROCENTA_RE.finditer(s):
        v = _to_float(m.group(1))
        if v is not None:
            pct_total += v; pct_found = True
    if pct_found:
        return max(0.0, min(100.0, pct_total))

    return None


def _compose_share_raw(podil: Dict[str, Any]) -> str:
    """Sestaví sdružený text podílu (vklad / velikost / splaceno) pro UI/fallback."""
    parts = []
    if v := podil.get("vklad"):
        t = v.get("typObnos"); h = v.get("hodnota")
        if h is not None:
            parts.append(f"vklad:{h} {t or ''}".strip())
    if vp := podil.get("velikostPodilu"):
        t = vp.get("typObnos"); h = vp.get("hodnota")
        if h is not None:
            parts.append(f"velikost:{h} {t or ''}".strip())
    if s := podil.get("splaceni"):
        t = s.get("typObnos"); h = s.get("hodnota")
        if h is not None:
            parts.append(f"splaceno:{h} {t or ''}".strip())
    return "; ".join(parts)


def _parse_share_from_podil_list(podily: List[Dict[str, Any]]) -> Tuple[Optional[float], str]:
    """
    V každém 'podil' záznamu:
    - pokud je aktivní (bez datumVymazu), zahrneme,
    - pokusíme se převést procenta z VŠECH typů (PROCENTA / TEXT / zlomky),
    - více podílů se sečte (např. A/B + C/D), výsledek je v PROCENTECH 0..100,
    - 'raw' vrátíme pro UI/fallback.
    """
    pct_sum = 0.0
    pct_found = False
    raw_parts: List[str] = []

    for p in podily or []:
        if not _is_active_item(p):
            continue

        # slož raw pro UI/fallback
        raw_parts.append(_compose_share_raw(p))

        # najdi hodnotu v 'velikostPodilu'
        vp = p.get("velikostPodilu") or {}
        typ = (vp.get("typObnos") or "").upper()
        hod = vp.get("hodnota")

        if hod is not None:
            text_val = str(hod)
            # 1) PROCENTA: pokus přímo -> robustně (podpora ';' i ',')
            if typ == "PROCENTA":
                v = _to_float(text_val)
                if v is not None:
                    pct_sum += v; pct_found = True
                else:
                    # 2) když selže (např. '2;25'), zkus robustní parser na text
                    parsed = _parse_pct_from_text(text_val)
                    if parsed is not None:
                        pct_sum += parsed; pct_found = True
            else:
                # 3) TEXT / zlomky: robustní parser
                parsed = _parse_pct_from_text(text_val)
                if parsed is not None:
                    pct_sum += parsed; pct_found = True

    raw = "; ".join([r for r in raw_parts if r])[:1000]
    return (max(0.0, min(100.0, pct_sum)) if pct_found else None), raw


def _person_name(fos: Dict[str, Any]) -> str:
    parts = []
    if fos.get("titulPredJmenem"):
        parts.append(fos["titulPredJmenem"])
    if fos.get("jmeno"):
        parts.append(fos["jmeno"])
    if fos.get("prijmeni"):
        parts.append(fos["prijmeni"])
    return " ".join([p for p in parts if p]).strip() or "Fyzická osoba (neznámá)"


@dataclass
class Owner:
    kind: str               # "PERSON" | "COMPANY"
    name: str
    ico: Optional[str]      # jen u COMPANY (8 číslic, zfill)
    share_pct: Optional[float]
    share_raw: Optional[str]
    label: str              # "Společníci" | "Akcionáři"


def extract_current_owners(vr_payload: Dict[str, Any]) -> Tuple[str, str, List[Owner]]:
    """
    Vrací (company_ico, company_name, owners[]), pouze s AKTUÁLNÍMI podíly:
      - ignoruje záznamy s 'datumVymazu',
      - u stejného vlastníka (label + identifikátor) bere nejnovější 'datumZapisu',
      - převádí podíl na číslo (PROCENTA/TEXT/zlomky), v procentech 0..100,
      - IČO normalizuje na 8 míst (zfill).
    """
    company_ico = (vr_payload.get("icoId") or "").strip()
    company_ico = _normalize_ico(company_ico)
    zaznam = _pick_primary_or_record(vr_payload.get("zaznamy") or [])
    if not zaznam:
        return company_ico or "", "Neznámý subjekt", []

    # název – poslední aktivní obchodniJmeno, jinak poslední
    name = "Neznámý subjekt"
    oj = zaznam.get("obchodniJmeno") or []
    oj_active = [x for x in oj if _is_active_item(x)]
    if oj_active:
        name = oj_active[-1].get("hodnota") or name
    elif oj:
        name = oj[-1].get("hodnota") or name

    owners: List[Owner] = []

    # --- SPOLEČNÍCI ---
    # dedup: (label, identifikátor) -> nejnovější aktivní 'spolecnik'
    latest: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for blok in (zaznam.get("spolecnici") or []):
        if not _is_active_item(blok):
            continue
        label = blok.get("nazevOrganu") or "Společníci"
        for sp in (blok.get("spolecnik") or []):
            if not _is_active_item(sp):
                continue

            osoba = sp.get("osoba") or {}
            fos = osoba.get("fyzickaOsoba")
            pos = osoba.get("pravnickaOsoba")

            if pos:
                o_ico = _normalize_ico(pos.get("ico"))
                o_name = (pos.get("obchodniJmeno") or pos.get("nazev") or f"Společnost (IČO {o_ico or '?'})").strip()
                kind = "COMPANY"
                ident = f"{kind}:{o_ico or o_name}"
            elif fos:
                o_name = _person_name(fos)
                kind = "PERSON"
                o_ico = None
                ident = f"{kind}:{o_name}"
            else:
                # neznámé – přeskoč
                continue

            # vyber nejnovější aktivní záznam (bez datumVymazu)
            dz = _parse_date(sp.get("datumZapisu"))
            key = (label, ident)
            prev = latest.get(key)
            prev_dz = _parse_date(prev.get("datumZapisu")) if prev else None
            if (prev is None) or ((prev_dz or datetime.min) < (dz or datetime.min)):
                latest[key] = {
                    "label": label,
                    "kind": kind,
                    "name": o_name,
                    "ico": o_ico,
                    "spolecnik": sp,
                    "datumZapisu": sp.get("datumZapisu"),
                }

    # převod na Owner
    for (_, _), rec in latest.items():
        sp = rec["spolecnik"]
        podily = sp.get("podil") or []
        share_pct, raw = _parse_share_from_podil_list(podily)

        owners.append(
            Owner(
                kind=rec["kind"],
                name=rec["name"],
                ico=rec["ico"],
                share_pct=share_pct,       # např. 87.75, 10.0, 2.25 (v procentech)
                share_raw=raw or None,     # např. "velikost: 2;25 PROCENTA; splaceno:100 PROCENTA"
                label=rec["label"],
            )
        )

    # --- AKCIONÁŘI ---
    # V modelu OR „Jediný akcionář“ často nemá explicitní velikost podílu – použijeme 100 %.
    for org in (zaznam.get("akcionari") or []):
        if not _is_active_item(org):
            continue
        label = org.get("nazevOrganu") or "Akcionáři"
        for a in (org.get("clenoveOrganu") or []):
            if not _is_active_item(a):
                continue
            fos = a.get("fyzickaOsoba")
            pos = a.get("pravnickaOsoba")

            if pos:
                o_ico = _normalize_ico(pos.get("ico"))
                o_name = (pos.get("obchodniJmeno") or pos.get("nazev") or f"Společnost (IČO {o_ico or '?'})").strip()
                owners.append(Owner("COMPANY", o_name, o_ico, 100.0, None, label))
            elif fos:
                o_name = _person_name(fos)
                owners.append(Owner("PERSON", o_name, None, 100.0, None, label))

    return company_ico or "", name or "", owners
