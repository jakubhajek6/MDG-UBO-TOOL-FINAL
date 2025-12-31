# ownership_resolve_online.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple, Any, Union

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

ICO_RE = re.compile(r"^\d{7,8}$")
FOREIGN_ID_RE = re.compile(r"^[A-Za-z]{1,6}\d{3,}$")  # např. Z4159842, SK123456, DE12345...


def _to_float(s: str) -> Optional[float]:
    try:
        return float(s.replace(",", ".").replace(";", "."))
    except Exception:
        return None


def parse_pct_from_text(s: str) -> Optional[float]:
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

    # 2) hlasovaci_prava – %
    hv_total = 0.0; hv_found = False
    for m in HLASOVACI_PRAVA_PCT_RE.finditer(s):
        v = _to_float(m.group(1))
        if v is not None:
            hv_total += (v / 100.0); hv_found = True
    if hv_found:
        return max(0.0, min(1.0, hv_total))

    # 3) zlomky
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

    # 4) procenta
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
    s = (s or "").strip()
    m = EFEKTIVNE_RE.search(s)
    if m:
        v = _to_float(m.group(1))
        if v is not None:
            return max(0.0, min(1.0, v / 100.0))
    return None


def _is_cz_ico(x: str) -> bool:
    x = (x or "").strip()
    return bool(ICO_RE.match(x))


def _norm_ico(x: str) -> str:
    x = re.sub(r"\D", "", x or "")
    if len(x) == 7:
        x = "0" + x
    return x.zfill(8)


def _is_foreign_id(x: str) -> bool:
    x = (x or "").strip()
    if not x:
        return False
    if _is_cz_ico(re.sub(r"\D", "", x)):
        return False
    return bool(FOREIGN_ID_RE.match(x))


ManualOwner = Dict[str, Any]
ManualOverrides = Dict[str, List[ManualOwner]]


def _coerce_manual_overrides(
    manual_overrides: Optional[Union[Dict[str, List[Tuple[str, float]]], ManualOverrides]]
) -> ManualOverrides:
    """
    Podpora:
      - legacy: {ico: [(owner_ico, share_0..1), ...]}
      - new:    {node_id: [ {"type": "...", ...}, ... ]}

    Normalizovaný výstup:
      {"type": "company", "ico": "12345678", "share": 0.5, "name": Optional[str]}
      {"type": "foreign", "id": "Z4159842", "share": 0.5, "name": Optional[str]}
      {"type": "person",  "name": "Ing. Jan Novák", "share": 0.2}
    """
    if not manual_overrides:
        return {}

    out: ManualOverrides = {}
    for k, lst in manual_overrides.items():
        key = str(k).strip()
        if not key:
            continue

        out_list: List[ManualOwner] = []
        for item in (lst or []):
            # legacy tuple/list: (ico, share)
            if isinstance(item, (tuple, list)):
                if len(item) < 2:
                    continue
                ico = _norm_ico(str(item[0]))
                try:
                    share = float(item[1] or 0.0)
                except Exception:
                    share = 0.0
                if share > 0:
                    out_list.append({"type": "company", "ico": ico, "share": share, "name": None})
                continue

            if not isinstance(item, dict):
                continue

            t_raw = (item.get("type") or "").strip()
            t = t_raw.lower()

            # typy z appky: CZ/FOREIGN/PERSON apod.
            if t in ("cz", "company", "comp"):
                t = "company"
            elif t in ("foreign", "z", "foreign_id"):
                t = "foreign"
            elif t in ("person", "fo"):
                t = "person"

            try:
                share = float(item.get("share") or 0.0)
            except Exception:
                share = 0.0
            if share <= 0:
                continue

            if t == "company":
                ico = item.get("ico") or item.get("id")
                if ico:
                    out_list.append({"type": "company", "ico": _norm_ico(str(ico)), "share": share, "name": item.get("name")})
                continue

            if t == "foreign":
                fid = item.get("id") or item.get("fid") or item.get("code")
                if fid:
                    out_list.append({"type": "foreign", "id": str(fid).strip(), "share": share, "name": item.get("name")})
                continue

            if t == "person":
                nm = item.get("name")
                if nm:
                    out_list.append({"type": "person", "name": str(nm).strip(), "share": share})
                continue

            # fallback heuristika
            if item.get("ico") or item.get("id"):
                # když je to číslo -> company, jinak foreign
                raw = str(item.get("ico") or item.get("id")).strip()
                digits = re.sub(r"\D+", "", raw)
                if digits.isdigit() and len(digits) in (7, 8):
                    out_list.append({"type": "company", "ico": _norm_ico(digits), "share": share, "name": item.get("name")})
                else:
                    out_list.append({"type": "foreign", "id": raw, "share": share, "name": item.get("name")})
            elif item.get("name"):
                out_list.append({"type": "person", "name": str(item["name"]).strip(), "share": share})

        if out_list:
            out[key] = out_list

    return out


def resolve_tree_online(
    client: AresVrClient,
    root_ico: str,
    max_depth: int = 25,
    manual_overrides: Optional[Union[Dict[str, List[Tuple[str, float]]], ManualOverrides]] = None,
) -> Tuple[List[NodeLine], List[Dict]]:
    lines: List[NodeLine] = []
    warnings: List[Dict] = []

    mo: ManualOverrides = _coerce_manual_overrides(manual_overrides)

    def walk_cz_company(ico: str, depth: int, parent_multiplier: float):
        nonlocal lines, warnings, mo

        if depth > max_depth:
            lines.append(NodeLine(depth, "", "⚠️ Překročena max hloubka", None))
            return

        ico_norm = _norm_ico(ico)

        payload = client.get_vr(ico_norm)
        if payload.get("_error"):
            err_txt = f"⚠️ Nelze načíst ARES VR pro {ico_norm}: {payload.get('_error')}"
            lines.append(NodeLine(depth, "", err_txt, None))
            warnings.append({"kind": "error", "ico": ico_norm, "name": "", "text": err_txt})
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

        # --- manuální doplnění vlastníků pro tuto CZ firmu ---
        manual_for_this = mo.get(c_ico, [])
        manual_owners: List[Owner] = []

        for it in manual_for_this:
            t = (it.get("type") or "").lower().strip()
            share = float(it.get("share") or 0.0)
            if share <= 0:
                continue

            if t == "company":
                owner_ico = _norm_ico(str(it.get("ico") or ""))
                if not owner_ico:
                    continue
                o_name_final = (it.get("name") or "").strip() or None

                # CZ firmy se můžou dohledat přes ARES (pokud není jméno)
                if not o_name_final:
                    o_name_final = f"Společnost (IČO {owner_ico})"
                    try:
                        p2 = client.get_vr(owner_ico)
                        if not p2.get("_error"):
                            _ico2, _name2, _ = extract_current_owners(p2)
                            if _name2:
                                o_name_final = _name2
                    except Exception:
                        pass

                manual_owners.append(
                    Owner(
                        kind="COMPANY",
                        name=o_name_final,
                        ico=owner_ico,
                        share_pct=share * 100.0,
                        share_raw=f"velikost:{share*100.0:.2f} PROCENTA",
                        label="Manuálně doplněno",
                    )
                )
                continue

            if t == "foreign":
                fid = str(it.get("id") or "").strip()
                if not fid:
                    continue
                nm = (it.get("name") or f"Zahraniční subjekt {fid}").strip()
                manual_owners.append(
                    Owner(
                        kind="FOREIGN",
                        name=nm,
                        ico=fid,  # používáme jako identifikátor
                        share_pct=share * 100.0,
                        share_raw=f"velikost:{share*100.0:.2f} PROCENTA",
                        label="Manuálně doplněno",
                    )
                )
                continue

            if t == "person":
                nm = str(it.get("name") or "").strip()
                if not nm:
                    continue
                manual_owners.append(
                    Owner(
                        kind="PERSON",
                        name=nm,
                        ico=None,
                        share_pct=share * 100.0,
                        share_raw=f"velikost:{share*100.0:.2f} PROCENTA",
                        label="Manuálně doplněno",
                    )
                )
                continue

        # APPEND režim (ARES + manuál)
        if manual_owners:
            owners = list(owners) + manual_owners

        if not owners:
            msg = f"⚠️ Nepodařilo se dohledat vlastníka v OR pro {c_name} (IČO {c_ico})"
            warnings.append({"kind": "unresolved", "id": c_ico, "ico": c_ico, "name": c_name, "text": msg})

        _emit_owners_and_recurse(
            owners=owners,
            depth=depth,
            parent_multiplier=parent_multiplier,
            warnings=warnings,
            walk_cz_company=walk_cz_company,
            walk_foreign=walk_foreign,
        )

    def walk_foreign(fid: str, depth: int, parent_multiplier: float, display_name: Optional[str] = None):
        nonlocal lines, warnings, mo

        if depth > max_depth:
            lines.append(NodeLine(depth, "", "⚠️ Překročena max hloubka", None))
            return

        fid = str(fid or "").strip()
        if not fid:
            lines.append(NodeLine(depth, "", "⚠️ Neplatný identifikátor zahraničního subjektu", None))
            return

        nm = (display_name or f"Zahraniční subjekt {fid}").strip()

        lines.append(NodeLine(depth, "", f"{nm} (ID {fid})", None))

        manual_for_this = mo.get(fid, [])
        owners: List[Owner] = []

        for it in manual_for_this:
            t = (it.get("type") or "").lower().strip()
            share = float(it.get("share") or 0.0)
            if share <= 0:
                continue

            if t == "company":
                owner_ico = _norm_ico(str(it.get("ico") or ""))
                if not owner_ico:
                    continue

                o_name_final = (it.get("name") or "").strip() or None
                if not o_name_final:
                    o_name_final = f"Společnost (IČO {owner_ico})"
                    try:
                        p2 = client.get_vr(owner_ico)
                        if not p2.get("_error"):
                            _ico2, _name2, _ = extract_current_owners(p2)
                            if _name2:
                                o_name_final = _name2
                    except Exception:
                        pass

                owners.append(
                    Owner(
                        kind="COMPANY",
                        name=o_name_final,
                        ico=owner_ico,
                        share_pct=share * 100.0,
                        share_raw=f"velikost:{share*100.0:.2f} PROCENTA",
                        label="Manuálně doplněno",
                    )
                )
                continue

            if t == "foreign":
                fid2 = str(it.get("id") or "").strip()
                if not fid2:
                    continue
                nm2 = (it.get("name") or f"Zahraniční subjekt {fid2}").strip()
                owners.append(
                    Owner(
                        kind="FOREIGN",
                        name=nm2,
                        ico=fid2,
                        share_pct=share * 100.0,
                        share_raw=f"velikost:{share*100.0:.2f} PROCENTA",
                        label="Manuálně doplněno",
                    )
                )
                continue

            if t == "person":
                nm3 = str(it.get("name") or "").strip()
                if not nm3:
                    continue
                owners.append(
                    Owner(
                        kind="PERSON",
                        name=nm3,
                        ico=None,
                        share_pct=share * 100.0,
                        share_raw=f"velikost:{share*100.0:.2f} PROCENTA",
                        label="Manuálně doplněno",
                    )
                )
                continue

        if not owners:
            msg = f"⚠️ Zahraniční subjekt {nm} (ID {fid}) nemá zadané vlastníky – doplň ručně."
            warnings.append({"kind": "unresolved", "id": fid, "ico": "", "name": nm, "text": msg})

        _emit_owners_and_recurse(
            owners=owners,
            depth=depth,
            parent_multiplier=parent_multiplier,
            warnings=warnings,
            walk_cz_company=walk_cz_company,
            walk_foreign=walk_foreign,
        )

    def _emit_owners_and_recurse(
        owners: List[Owner],
        depth: int,
        parent_multiplier: float,
        warnings: List[Dict],
        walk_cz_company,
        walk_foreign,
    ):
        by_label: Dict[str, list] = {}
        for o in owners:
            by_label.setdefault(o.label, []).append(o)

        for label, lst in by_label.items():
            lines.append(NodeLine(depth + 1, label, f"{label}:", None))

            for o in lst:
                local_share: Optional[float] = None
                eff_share: Optional[float] = None

                if getattr(o, "share_pct", None) is not None:
                    local_share = float(o.share_pct) / 100.0

                if local_share is None and getattr(o, "share_raw", None):
                    local_share = parse_pct_from_text(o.share_raw)

                eff_from_text = parse_effective_from_text(getattr(o, "share_raw", "") or "")
                if eff_from_text is not None:
                    eff_share = eff_from_text

                if local_share is not None:
                    next_mult = parent_multiplier * local_share
                elif eff_share is not None:
                    next_mult = eff_share
                else:
                    next_mult = parent_multiplier

                if local_share is not None:
                    pct_txt = f"{local_share * 100.0:.2f}%"
                    eff_pct = parent_multiplier * local_share * 100.0
                elif eff_share is not None:
                    pct_txt = getattr(o, "share_raw", None) or "?"
                    eff_pct = eff_share * 100.0
                else:
                    pct_txt = getattr(o, "share_raw", None) or "?"
                    eff_pct = None

                kind = (getattr(o, "kind", "") or "").upper()

                if kind == "COMPANY" and getattr(o, "ico", None):
                    lines.append(NodeLine(depth + 2, label, f"{o.name} — {pct_txt} (IČO {o.ico})", eff_pct))
                    walk_cz_company(o.ico, depth + 3, next_mult)
                    continue

                if kind == "FOREIGN" and getattr(o, "ico", None):
                    fid = str(getattr(o, "ico")).strip()
                    lines.append(NodeLine(depth + 2, label, f"{o.name} — {pct_txt} (ID {fid})", eff_pct))
                    walk_foreign(fid, depth + 3, next_mult, display_name=o.name)
                    continue

                # PERSON / fallback: vždy vypiš i efektivně, pokud známe lokální
                if local_share is None:
                    sr = (getattr(o, "share_raw", "") or "").strip()
                    local_share = parse_pct_from_text(sr)
                    if local_share is None and sr:
                        try:
                            v = float(sr.replace(",", ".").replace(";", "."))
                            local_share = max(0.0, min(1.0, v / 100.0))
                        except Exception:
                            pass

                if local_share is not None:
                    eff_pct2 = parent_multiplier * local_share * 100.0
                    base_pct = local_share * 100.0
                    lines.append(NodeLine(depth + 2, label, f"{o.name} — {base_pct:.2f}% (efektivně {eff_pct2:.2f}%)", eff_pct2))
                elif eff_share is not None:
                    base_txt = f"{float(o.share_pct):.2f}%" if getattr(o, "share_pct", None) is not None else (getattr(o, "share_raw", None) or "?")
                    lines.append(NodeLine(depth + 2, label, f"{o.name} — {base_txt} (efektivně {eff_share * 100.0:.2f}%)", eff_share * 100.0))
                else:
                    raw = f" — {getattr(o, 'share_raw', '')}" if getattr(o, "share_raw", None) else ""
                    lines.append(NodeLine(depth + 2, label, f"{o.name}{raw}", None))

    walk_cz_company(root_ico, depth=0, parent_multiplier=1.0)
    return lines, warnings
