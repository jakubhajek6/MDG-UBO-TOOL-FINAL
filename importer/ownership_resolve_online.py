
# ownership_resolve_online.py

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple

from importer.ares_vr_client import AresVrClient
from importer.ares_vr_extract import extract_current_owners, Owner


@dataclass
class NodeLine:
    depth: int
    label: str             # "Společníci" / "Akcionáři" / "" (hlavička)
    text: str              # co vypíšeme
    effective_pct: Optional[float]  # efektivní podíl v %, pokud znám (0..100)


# ===== Robustní parser podílů z TEXTU (OR) =====
PCT_RE = re.compile(r"(\d+(?:[.,;]\d+)?)\s*%")
PROCENTA_RE = re.compile(r"(\d+(?:[.,;]\d+)?)\s*PROCENTA", re.IGNORECASE)
FRAC_SLASH_RE = re.compile(r"(\d+)\s*/\s*(\d+)")
FRAC_SEMI_RE = re.compile(r"(\d+)\s*;\s*(\d+)\s*(ZLOMEK|TEXT)?", re.IGNORECASE)

OBCHODNI_PODIL_FRAC_RE = re.compile(r"obchodni[_ ]?podil\s*:\s*(\d+)\s*[/;]\s*(\d+)", re.IGNORECASE)
OBCHODNI_PODIL_PCT_RE = re.compile(r"obchodni[_ ]?podil\s*:\s*(\d+(?:[.,;]\d+)?)\s*(?:%|PROCENTA)", re.IGNORECASE)

HLASOVACI_PRAVA_PCT_RE = re.compile(r"hlasovaci[_ ]?prava\s*:\s*(\d+(?:[.,;]\d+)?)\s*(?:%|PROCENTA)", re.IGNORECASE)
SPLACENO_FIELD_RE = re.compile(r"splaceno\s*:\s*\d+(?:[.,;]\d+)?\s*PROCENTA", re.IGNORECASE)

EFEKTIVNE_RE = re.compile(r"efektivně\s+(\d+(?:[.,;]\d+)?)\s*%", re.IGNORECASE)


def _to_float(s: str) -> Optional[float]:
    try:
        return float(s.replace(",", ".").replace(";", "."))
    except Exception:
        return None


def parse_pct_from_text(s: str) -> Optional[float]:
    """
    Přetaví text OR na podíl 0..1 (tj. 33 % -> 0.33, 1/3 -> 0.3333…).
    Logika:
      1) sečte VŠECHNY výskyty 'obchodni_podil' (zlomky i %), ignoruje 'splaceno:… PROCENTA',
      2) pokud 'obchodni_podil' chybí, sečte VŠECHNY 'hlasovaci_prava' (%),
      3) pak obecné zlomky ('a/b', 'a;b') – všechny výskyty,
      4) nakonec obecné procenta ('X %' / 'X PROCENTA') – všechny výskyty.
    Výsledek zastropuje na [0,1]. Vrací None, pokud nic nenajde.
    """
    s = (s or "").strip()
    if not s:
        return None

    s = SPLACENO_FIELD_RE.sub("", s)

    # 1) obchodni_podil – zlomek + %
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

    # 2) explicitní 'hlasovaci_prava' – sečti všechny výskyty
    hv_total = 0.0; hv_found = False
    for m in HLASOVACI_PRAVA_PCT_RE.finditer(s):
        v = _to_float(m.group(1))
        if v is not None:
            hv_total += (v / 100.0); hv_found = True
    if hv_found:
        return max(0.0, min(1.0, hv_total))

    # 3) obecné zlomky – a/b, a;b
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

    # 4) obecná procenta – X%, X PROCENTA
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


def parse_effective_from_text(s: str) -> Optional[float]:
    """
    Najde 'efektivně X %' a vrátí X/100 (tj. 0..1). Jinak None.
    """
    s = (s or "").strip()
    m = EFEKTIVNE_RE.search(s)
    if m:
        v = _to_float(m.group(1))
        if v is not None:
            return max(0.0, min(1.0, v / 100.0))
    return None


def resolve_tree_online(
    client: AresVrClient,
    root_ico: str,
    max_depth: int = 25,
    manual_overrides: Optional[Dict[str, List[Tuple[str, float]]]] = None,
) -> Tuple[List[NodeLine], List[Dict]]:
    """
    Rozkryje vlastnickou strukturu přes ARES VR API.

    ZÁMĚRNĚ:
    - bez detekce cyklů / duplicit (AML: rozbalit cestu pokaždé),
    - ochrana jen přes max_depth,
    - manual_overrides: {target_company_ico: [(owner_ico, share_0..1), ...]}.
      Výchozí režim je APPEND (ARES + manuál).
    """
    lines: List[NodeLine] = []
    warnings: List[Dict] = []

    def walk(ico: str, depth: int, parent_multiplier: float):
        nonlocal lines, warnings

        if depth > max_depth:
            lines.append(NodeLine(depth, "", "⚠️ Překročena max hloubka", None))
            return

        payload = client.get_vr(ico)
        if payload.get("_error"):
            err_txt = f"⚠️ Nelze načíst ARES VR pro {ico}: {payload.get('_error')}"
            lines.append(NodeLine(depth, "", err_txt, None))
            warnings.append({"kind": "error", "ico": ico, "name": "", "text": err_txt})
            return

        c_ico, c_name, owners = extract_current_owners(payload)

        # Hlavička firmy
        lines.append(
            NodeLine(
                depth,
                "",
                f"{c_name} (IČO {c_ico})",
                parent_multiplier * 100.0 if depth == 0 else None,
            )
        )

        # --- manuální doplnění vlastníků pro tuto firmu ---
        manual_for_this = (manual_overrides or {}).get(c_ico, [])
        manual_owners: List[Owner] = []
        for owner_ico, owner_share in manual_for_this:
            o_name_final = f"Společnost (IČO {str(owner_ico).zfill(8)})"
            try:
                p2 = client.get_vr(owner_ico)
                _ico2, _name2, _ = extract_current_owners(p2)
                if _name2:
                    o_name_final = _name2
            except Exception:
                pass
            manual_owners.append(
                Owner(
                    kind="COMPANY",
                    name=o_name_final,
                    ico=str(owner_ico).zfill(8),
                    share_pct=owner_share * 100.0,                           # v procentech
                    share_raw=f"velikost:{owner_share*100.0:.2f} PROCENTA",  # pro UI/fallback
                    label="Manuálně doplněno",
                )
            )

        # APPEND režim (ARES + manuál). Pokud chceš REPLACE, nastav owners = manual_owners
        if manual_owners:
            owners = list(owners) + manual_owners

        # Pokud po ARES + manuálu neexistuje žádný vlastník, označ jako 'unresolved'
        if not owners:
            msg = f"⚠️ Nepodařilo se dohledat vlastníka v OR pro {c_name} (IČO {c_ico})"
            warnings.append({"kind": "unresolved", "ico": c_ico, "name": c_name, "text": msg})

        # seskupíme podle labelu (Společníci / Akcionáři / Manuálně doplněno)
        by_label: Dict[str, list] = {}
        for o in owners:
            by_label.setdefault(o.label, []).append(o)

        for label, lst in by_label.items():
            lines.append(NodeLine(depth + 1, label, f"{label}:", None))

            for o in lst:
                # === 1) Získej lokální podíl (0..1) ===
                local_share: Optional[float] = None      # lokální (na této úrovni)
                eff_share: Optional[float] = None        # efektivní (násobeno rodičem)

                if getattr(o, "share_pct", None) is not None:
                    local_share = float(o.share_pct) / 100.0

                if local_share is None and getattr(o, "share_raw", None):
                    local_share = parse_pct_from_text(o.share_raw)

                # 'efektivně X %' v textu – už násobeno rodičem
                eff_from_text = parse_effective_from_text(getattr(o, "share_raw", "") or "")
                if eff_from_text is not None:
                    eff_share = eff_from_text

                # === 2) Vytvoř řádku NodeLine a spočti efektivní podíl ===
                if getattr(o, "kind", "") == "COMPANY" and getattr(o, "ico", None):
                    # Firma-vlastník
                    if local_share is not None:
                        pct_txt = f"{local_share * 100.0:.2f}%"
                        eff_pct = parent_multiplier * local_share * 100.0
                    elif eff_share is not None:
                        pct_txt = getattr(o, "share_raw", None) or "?"
                        eff_pct = eff_share * 100.0
                    else:
                        pct_txt = getattr(o, "share_raw", None) or "?"
                        eff_pct = None

                    lines.append(
                        NodeLine(
                            depth + 2,
                            label,
                            f"{o.name} — {pct_txt} (IČO {o.ico})",
                            eff_pct,
                        )
                    )

                    # rekurze: multiplikátor pro dceřinou hlavičku
                    if local_share is not None:
                        next_mult = parent_multiplier * local_share
                    elif eff_share is not None:
                        next_mult = eff_share
                    else:
                        next_mult = parent_multiplier  # neznámé — pokračuj bez násobení

                    walk(o.ico, depth + 3, next_mult)

                else:
                    # Fyzická osoba
                    if local_share is not None:
                        eff_pct = parent_multiplier * local_share * 100.0
                        lines.append(
                            NodeLine(
                                depth + 2,
                                label,
                                f"{o.name} — {local_share * 100.0:.2f}% (efektivně {eff_pct:.2f}%)",
                                eff_pct,
                            )
                        )
                    elif eff_share is not None:
                        if getattr(o, "share_pct", None) is not None:
                            base_txt = f"{float(o.share_pct):.2f}%"
                        else:
                            base_txt = getattr(o, "share_raw", None) or "?"
                        lines.append(
                            NodeLine(
                                depth + 2,
                                label,
                                f"{o.name} — {base_txt} (efektivně {eff_share * 100.0:.2f}%)",
                                eff_share * 100.0,
                            )
                        )
                    else:
                        raw = f" — {getattr(o, 'share_raw', '')}" if getattr(o, "share_raw", None) else ""
                        lines.append(NodeLine(depth + 2, label, f"{o.name}{raw}", None))

    walk(root_ico, depth=0, parent_multiplier=1.0)
    return lines, warnings
