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
FOREIGN_ID_RE = re.compile(r"^[A-Za-z]{1,6}\d{3,}$")  # např. Z4159842, SK123456, DE12345 apod.


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
            if isinstance(item, tuple) or isinstance(item, list):
                # legacy tuple: (ico, share)
                if len(item) < 2:
                    continue
                ico = _norm_ico(str(item[0]))
                share = float(item[1] or 0.0)
                out_list.append({"type": "company", "ico": ico, "share": share})
                continue

            if isinstance(item, dict):
                t = (item.get("type") or "").strip().lower()
                share = float(item.get("share") or 0.0)
                if share <= 0:
                    continue

                if t == "company":
                    ico = item.get("ico")
                    if ico:
                        out_list.append({"type": "company", "ico": _norm_ico(str(ico)), "share": share, "name": item.get("name")})
                elif t == "foreign":
                    fid = item.get("id") or item.get("fid") or item.get("code")
                    if fid:
                        out_list.append({"type": "foreign", "id": str(fid).strip(), "share": share, "name": item.get("name")})
                elif t == "person":
                    nm = item.get("name")
                    if nm:
                        out_list.append({"type": "person", "name": str(nm).strip(), "share": share})
                else:
                    # fallback: zkus heuristiku
                    if item.get("ico"):
                        out_list.append({"type": "company", "ico": _norm_ico(str(item["ico"])), "share": share, "name": item.get("name")})
                    elif item.get("id"):
                        out_list.append({"type": "foreign", "id": str(item["id"]).strip(), "share": share, "name": item.get("name")})
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
    """
    Rozkryje vlastnickou strukturu přes ARES VR API.

    - max_depth: ochrana proti nekonečnému rozbalování
    - manual_overrides:
        legacy: {target_ico: [(owner_ico, share_0..1), ...]}
        new:    {node_id: [ {"type": company|foreign|person, ...}, ... ]}
      Režim je APPEND (ARES + manuál).
      Pro zahraniční uzly se ARES NEVOLÁ a struktura je čistě manuální.
    """
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
        manual_for_this = (manual_overrides or {}).get(c_ico, [])
        manual_owners: List[Owner] = []

        for item in (manual_for_this or []):
            # zpětná kompatibilita: když někde zůstaly tuple (ico, share)
            if isinstance(item, (tuple, list)) and len(item) >= 2:
                item = {"type": "CZ", "id": str(item[0]), "name": None, "share": float(item[1])}

            itype = (item.get("type") or "CZ").upper()
            oid = item.get("id")
            oname = (item.get("name") or "").strip() or None
            oshare = float(item.get("share") or 0.0)

            if oshare <= 0:
                continue

            # 1) FO – nikdy se nedohledává ARES, je to konec větve
            if itype == "PERSON":
                pname = (item.get("name") or "").strip()
                if not pname:
                    continue
                manual_owners.append(
                    Owner(
                        kind="PERSON",
                        name=pname,
                        ico=None,
                        share_pct=oshare * 100.0,
                        share_raw=f"{oshare*100.0:.2f} %",
                        label="Manuálně doplněno",
                    )
                )
                continue

            # 2) FOREIGN – Zxxxx… nikdy nevolej ARES a nikdy nerekurzuj automaticky
            if itype == "FOREIGN":
                zid = str(oid or "").strip().upper()
                if not zid:
                    continue
                display = oname or f"Zahraniční subjekt ({zid})"
                manual_owners.append(
                    Owner(
                        kind="FOREIGN",
                        name=display,
                        ico=zid,  # držíme Z-ID v poli ico (jen jako identifikátor)
                        share_pct=oshare * 100.0,
                        share_raw=f"{oshare*100.0:.2f} %",
                        label="Manuálně doplněno",
                    )
                )
                continue

            # 3) CZ firma – můžeš zkusit dohledat název přes ARES (pokud není vyplněn)
            ico_raw = str(oid or "").strip()
            ico_clean = re.sub(r"\D+", "", ico_raw).zfill(8)
            if not ico_clean.isdigit() or len(ico_clean) != 8:
                continue

            o_name_final = oname or f"Společnost (IČO {ico_clean})"
            if oname is None:
                try:
                    p2 = client.get_vr(ico_clean)
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
                    ico=ico_clean,
                    share_pct=oshare * 100.0,
                    share_raw=f"velikost:{oshare*100.0:.2f} PROCENTA",
                    label="Manuálně doplněno",
                )
            )


            elif t == "foreign":
                fid = str(it.get("id") or "").strip()
                if not fid:
                    continue
                nm = (it.get("name") or f"Zahraniční subjekt {fid}").strip()
                manual_owners_from_mo.append(
                    Owner(
                        kind="FOREIGN",
                        name=nm,
                        ico=fid,  # záměrně používáme pole ico jako identifikátor
                        share_pct=share * 100.0,
                        share_raw=f"velikost:{share*100.0:.2f} PROCENTA",
                        label="Manuálně doplněno",
                    )
                )

            elif t == "person":
                nm = str(it.get("name") or "").strip()
                if not nm:
                    continue
                manual_owners_from_mo.append(
                    Owner(
                        kind="PERSON",
                        name=nm,
                        ico=None,
                        share_pct=share * 100.0,
                        share_raw=f"velikost:{share*100.0:.2f} PROCENTA",
                        label="Manuálně doplněno",
                    )
                )

        # APPEND režim (ARES + manuál)
        if manual_owners_from_mo:
            owners = list(owners) + manual_owners_from_mo

        # Pokud po ARES + manuálu neexistuje žádný vlastník, označ jako 'unresolved'
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
            max_depth=max_depth,
            client=client,
            mo=mo,
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

        # Hlavička "foreign entity"
        lines.append(
            NodeLine(
                depth,
                "",
                f"{nm} (ID {fid})",
                None,
            )
        )

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
                o_name_final = it.get("name") or f"Společnost (IČO {owner_ico})"
                try:
                    p2 = client.get_vr(owner_ico)
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

            elif t == "foreign":
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

            elif t == "person":
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
            max_depth=max_depth,
            client=client,
            mo=mo,
        )

    def _emit_owners_and_recurse(
        owners: List[Owner],
        depth: int,
        parent_multiplier: float,
        warnings: List[Dict],
        walk_cz_company,
        walk_foreign,
        max_depth: int,
        client: AresVrClient,
        mo: ManualOverrides,
    ):
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

                # multiplikátor pro další úroveň
                if local_share is not None:
                    next_mult = parent_multiplier * local_share
                elif eff_share is not None:
                    next_mult = eff_share
                else:
                    next_mult = parent_multiplier

                # text podílu
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
                    # CZ firma
                    lines.append(
                        NodeLine(
                            depth + 2,
                            label,
                            f"{o.name} — {pct_txt} (IČO {o.ico})",
                            eff_pct,
                        )
                    )
                    walk_cz_company(o.ico, depth + 3, next_mult)

                elif kind == "FOREIGN" and getattr(o, "ico", None):
                    fid = str(getattr(o, "ico")).strip()
                    # foreign uzel (ID)
                    lines.append(
                        NodeLine(
                            depth + 2,
                            label,
                            f"{o.name} — {pct_txt} (ID {fid})",
                            eff_pct,
                        )
                    )
                    walk_foreign(fid, depth + 3, next_mult, display_name=o.name)

                else:
                    # Fyzická osoba

                    # pokus o robustní získání lokálního podílu (0..1)
                    if local_share is None:
                        sr = (getattr(o, "share_raw", "") or "").strip()

                        # 1) standardní parser (umí % / PROCENTA / zlomky…)
                        local_share = parse_pct_from_text(sr)

                        # 2) fallback: čisté číslo bez % ber jako procenta (např. "100" nebo "50.0")
                        if local_share is None and sr:
                            try:
                                v = float(sr.replace(",", ".").replace(";", "."))
                                # interpretace jako procenta
                                local_share = max(0.0, min(1.0, v / 100.0))
                            except Exception:
                                pass

                    # pokud umíme lokální podíl -> dopočti efektivní a vypiš vždy
                    if local_share is not None:
                        eff_pct = parent_multiplier * local_share * 100.0
                        base_pct = local_share * 100.0

                        # pokud je to "root" (parent_multiplier ~ 1), můžeš chtít jen 100% bez "efektivně"
                        # ale ty chceš mít efektivně vždy, takže to vypíšeme vždy:
                        lines.append(
                            NodeLine(
                                depth + 2,
                                label,
                                f"{o.name} — {base_pct:.2f}% (efektivně {eff_pct:.2f}%)",
                                eff_pct,
                            )
                        )

                    # pokud nemáme lokální podíl, ale máme "efektivně X%" v textu
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

                    # úplný fallback – vypiš raw
                    else:
                        raw = f" — {getattr(o, 'share_raw', '')}" if getattr(o, "share_raw", None) else ""
                        lines.append(NodeLine(depth + 2, label, f"{o.name}{raw}", None))


    # start
    walk_cz_company(root_ico, depth=0, parent_multiplier=1.0)
    return lines, warnings
