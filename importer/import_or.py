import argparse
import gzip
import re
import sqlite3
from pathlib import Path
from typing import Optional, Tuple, List, Dict

from lxml import etree

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "database.sqlite"
SCHEMA_PATH = BASE_DIR / "db" / "schema.sql"


# ---------------------------
# DB helpers
# ---------------------------

def init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        con.commit()


def upsert_company(con, ico: str, name: str):
    con.execute(
        "INSERT INTO company(ico, name) VALUES(?, ?) "
        "ON CONFLICT(ico) DO UPDATE SET name=excluded.name",
        (ico, name),
    )


def get_or_create_entity_company(con, ico: str, name: str) -> int:
    row = con.execute(
        "SELECT entity_id FROM entity WHERE type='COMPANY' AND ico=?",
        (ico,),
    ).fetchone()
    if row:
        return row[0]
    cur = con.execute(
        "INSERT INTO entity(type, ico, name) VALUES('COMPANY', ?, ?)",
        (ico, name),
    )
    return cur.lastrowid


def get_or_create_entity_person(con, name: str) -> int:
    row = con.execute(
        "SELECT entity_id FROM entity WHERE type='PERSON' AND name=?",
        (name,),
    ).fetchone()
    if row:
        return row[0]
    cur = con.execute(
        "INSERT INTO entity(type, ico, name) VALUES('PERSON', NULL, ?)",
        (name,),
    )
    return cur.lastrowid


def delete_edges_for_company(con, target_ico: str):
    con.execute("DELETE FROM ownership_edge WHERE target_ico=?", (target_ico,))


def insert_edge(
    con,
    target_ico: str,
    owner_entity_id: int,
    share_pct=None,
    share_raw=None,
):
    con.execute(
        """
        INSERT INTO ownership_edge(
            target_ico,
            owner_entity_id,
            share_pct,
            share_raw
        )
        VALUES (?, ?, ?, ?)
        """,
        (target_ico, owner_entity_id, share_pct, share_raw),
    )


# ---------------------------
# XML helpers
# ---------------------------

def strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def norm_ico(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    digits = re.sub(r"\D+", "", s)
    if not digits:
        return None
    # standard IČO = 8 číslic
    return digits.zfill(8)


def text_of(elem: Optional[etree._Element]) -> Optional[str]:
    if elem is None:
        return None
    t = (elem.text or "").strip()
    return t if t else None


def first_xpath_text(elem: etree._Element, xpath: str) -> Optional[str]:
    found = elem.xpath(xpath)
    if not found:
        return None
    if isinstance(found[0], etree._Element):
        return text_of(found[0])
    s = str(found[0]).strip()
    return s if s else None


def udaj_kod(udaj_elem: etree._Element) -> Optional[str]:
    return first_xpath_text(udaj_elem, "./udajTyp/kod")


def extract_company_ico_and_name(subjekt: etree._Element) -> Tuple[Optional[str], Optional[str]]:
    ico = norm_ico(first_xpath_text(subjekt, "./ico"))
    name = first_xpath_text(subjekt, "./nazev")
    if not name:
        name = first_xpath_text(subjekt, ".//Udaj[udajTyp/kod='NAZEV']/hodnotaText")
    return ico, name


def extract_share_from_spolecnik_udaj(spolecnik_udaj: etree._Element) -> Tuple[Optional[float], Optional[str]]:
    """
    Společník může mít více podílů (A/B/C...). Sečteme procenta.
    """
    pct_sum: float = 0.0
    pct_found = False
    raw_parts: List[str] = []

    podil_udaje = spolecnik_udaj.xpath(".//Udaj[udajTyp/kod='SPOLECNIK_PODIL']")
    for pu in podil_udaje:
        vklad_typ = first_xpath_text(pu, "./hodnotaUdaje/vklad/typ")
        vklad_val = first_xpath_text(pu, "./hodnotaUdaje/vklad/textValue")
        if vklad_typ and vklad_val:
            raw_parts.append(f"vklad:{vklad_val} {vklad_typ}")

        souhrn_typ = first_xpath_text(pu, "./hodnotaUdaje/souhrn/typ")
        souhrn_val = first_xpath_text(pu, "./hodnotaUdaje/souhrn/textValue")
        if souhrn_typ and souhrn_val:
            raw_parts.append(f"obchodni_podil:{souhrn_val} {souhrn_typ}")

        splac_typ = first_xpath_text(pu, "./hodnotaUdaje/splaceni/typ")
        splac_val = first_xpath_text(pu, "./hodnotaUdaje/splaceni/textValue")
        if splac_typ and splac_val:
            raw_parts.append(f"splaceno:{splac_val} {splac_typ}")

        druh = first_xpath_text(pu, "./hodnotaUdaje/druhPodilu")
        if druh:
            raw_parts.append(f"druh:{druh}")

        if (souhrn_typ or "").upper() == "PROCENTA" and souhrn_val:
            try:
                val = float(souhrn_val.replace(",", "."))
                if 0 <= val <= 100:
                    pct_sum += val
                    pct_found = True
            except Exception:
                pass

    share_pct = pct_sum if pct_found else None
    share_raw = "; ".join(raw_parts)[:1000] if raw_parts else None
    return share_pct, share_raw


def extract_owner_from_spolecnik_udaj(spolecnik_udaj: etree._Element) -> Tuple[str, Optional[str], str]:
    """
    Vrací (owner_name, owner_ico_or_none, owner_kind)
      owner_kind = "PERSON" / "COMPANY"

    Podporuje i firmu uloženou v <osoba> jako:
      <osoba><nazev>...</nazev><ico>...</ico></osoba>
    """
    # 1) Fyzická osoba: jméno + příjmení
    jmeno = first_xpath_text(spolecnik_udaj, "./osoba/jmeno")
    prijmeni = first_xpath_text(spolecnik_udaj, "./osoba/prijmeni")
    if jmeno or prijmeni:
        name = " ".join([x for x in [jmeno, prijmeni] if x]).strip()
        return name, None, "PERSON"

    # 2) Firma uvnitř <osoba>
    osoba_nazev = first_xpath_text(spolecnik_udaj, "./osoba/nazev")
    osoba_ico = norm_ico(first_xpath_text(spolecnik_udaj, "./osoba/ico"))
    if osoba_nazev and osoba_ico:
        return osoba_nazev, osoba_ico, "COMPANY"

    # 3) Obecná právnická osoba
    owner_ico = norm_ico(first_xpath_text(spolecnik_udaj, ".//ico"))
    owner_name = (
        first_xpath_text(spolecnik_udaj, ".//nazev")
        or first_xpath_text(spolecnik_udaj, ".//obchodniFirma")
        or first_xpath_text(spolecnik_udaj, ".//firma")
        or first_xpath_text(spolecnik_udaj, "./hodnotaText")
    )
    if not owner_name and owner_ico:
        owner_name = f"Společník (IČO {owner_ico})"
    if not owner_name:
        owner_name = "Společník (neznámý)"
    return owner_name, owner_ico, "COMPANY"


def extract_partners_from_subjekt(subjekt: etree._Element) -> List[Dict]:
    """
    Načte vlastníky z OR dumpu pro:
      - s.r.o. (SPOLECNIK)
      - a.s. (AKCIONAR_SEKCE -> AKCIONAR), včetně "Jediný akcionář"

    Výstup je jednotný: list {kind, ico, name, share_pct, share_raw}
    """
    partners: List[Dict] = []

    # 1) s.r.o. a spol. — "Společníci" blok
    spolecnici_blocks = subjekt.xpath(".//Udaj[udajTyp/kod='SPOLECNIK']")
    for block in spolecnici_blocks:
        candidates = block.xpath("./podudaje/Udaj")
        for pu in candidates:
            k = (udaj_kod(pu) or "").upper()
            if not k.startswith("SPOLECNIK_"):
                continue
            if k == "SPOLECNIK_PODIL":
                continue

            owner_name, owner_ico, owner_kind = extract_owner_from_spolecnik_udaj(pu)
            share_pct, share_raw = extract_share_from_spolecnik_udaj(pu)

            partners.append(
                {
                    "kind": owner_kind,
                    "ico": owner_ico,
                    "name": owner_name,
                    "share_pct": share_pct,
                    "share_raw": share_raw,
                }
            )

    # 2) a.s. — "Akcionář" sekce (v tvém souboru: hlavicka "Jediný akcionář", kod AKCIONAR_SEKCE)
    akcionar_sections = subjekt.xpath(".//Udaj[udajTyp/kod='AKCIONAR_SEKCE']")
    for sec in akcionar_sections:
        sec_header = (first_xpath_text(sec, "./hlavicka") or "").strip().lower()

        candidates = sec.xpath("./podudaje/Udaj")
        for pu in candidates:
            k = (udaj_kod(pu) or "").upper()

            # v tvém výpisu je uvnitř přímo <kod>AKCIONAR</kod>
            if k != "AKCIONAR" and not k.startswith("AKCIONAR_"):
                continue
            # kdyby existovalo něco jako AKCIONAR_PODIL, tak to přeskočíme (analogicky)
            if k.endswith("_PODIL"):
                continue

            owner_name, owner_ico, owner_kind = extract_owner_from_spolecnik_udaj(pu)
            share_pct, share_raw = extract_share_from_spolecnik_udaj(pu)

            # "Jediný akcionář" → pokud není podíl vyplněn, nastav 100 %
            if share_pct is None and ("jediný akcionář" in sec_header or "jediny akcionar" in sec_header):
                share_pct = 100.0

            partners.append(
                {
                    "kind": owner_kind,
                    "ico": owner_ico,
                    "name": owner_name,
                    "share_pct": share_pct,
                    "share_raw": share_raw,
                }
            )

    # de-dup podle (ico,name,kind)
    uniq = {}
    for p in partners:
        key = (p["kind"], p["ico"] or "", p["name"])
        if key not in uniq:
            uniq[key] = p
        else:
            # když by se to sečetlo (např. více záznamů pro stejného akcionáře), tak agreguj procenta
            if uniq[key]["share_pct"] is None:
                uniq[key]["share_pct"] = p["share_pct"]
            elif p["share_pct"] is not None:
                uniq[key]["share_pct"] = float(uniq[key]["share_pct"]) + float(p["share_pct"])
            if p.get("share_raw"):
                uniq[key]["share_raw"] = (uniq[key].get("share_raw") or "") + ("; " if uniq[key].get("share_raw") else "") + p["share_raw"]

    return list(uniq.values())



def iter_records(xml_path: Path, record_tag: str):
    if xml_path.suffix.lower().endswith("gz"):
        fh = gzip.open(xml_path, "rb")
    else:
        fh = open(xml_path, "rb")

    context = etree.iterparse(
        fh,
        events=("end",),
        recover=True,
        huge_tree=True,
    )

    wanted = record_tag.lower()

    for _, elem in context:
        if strip_ns(elem.tag).lower() == wanted:
            yield elem
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

    fh.close()


# ---------------------------
# Library function for the app
# ---------------------------

def import_company(
    xml_path: Path,
    ico: str,
    record_tag: str = "Subjekt",
    replace: bool = True,
    db_path: Path = DB_PATH,
) -> bool:
    """
    Import exactly one company (ico) from one dump file into SQLite.
    Returns True if found+imported, False if not found in this dump.
    """
    ico = norm_ico(ico)
    init_db()

    scanned = 0
    with sqlite3.connect(db_path) as con:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")

        for subjekt in iter_records(Path(xml_path), record_tag=record_tag):
            scanned += 1
            c_ico, c_name = extract_company_ico_and_name(subjekt)
            if not c_ico:
                continue
            if c_ico != ico:
                continue

            upsert_company(con, c_ico, c_name or "")
            if replace:
                delete_edges_for_company(con, c_ico)

            partners = extract_partners_from_subjekt(subjekt)
            for p in partners:
                if p["kind"] == "COMPANY" and p["ico"]:
                    owner_id = get_or_create_entity_company(con, p["ico"], p["name"])
                else:
                    owner_id = get_or_create_entity_person(con, p["name"])

                insert_edge(
                    con,
                    target_ico=c_ico,
                    owner_entity_id=owner_id,
                    share_pct=p.get("share_pct"),
                    share_raw=p.get("share_raw"),
                )

            con.commit()
            return True

    return False


# ---------------------------
# CLI
# ---------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xml", required=True, help="Path to .xml or .xml.gz from dataor")
    ap.add_argument("--ico", help="If set: import only this ICO (TEST mode)")
    ap.add_argument("--record-tag", default="Subjekt", help="Top-level record tag (default: Subjekt)")
    ap.add_argument("--replace", action="store_true", help="Delete existing edges for that company before inserting")
    args = ap.parse_args()

    xml_path = Path(args.xml).expanduser().resolve()
    if not xml_path.exists():
        raise SystemExit(f"Soubor neexistuje: {xml_path}")

    if not args.ico:
        raise SystemExit("Pro CLI zatím podporuji jen --ico (import jedné firmy).")

    ok = import_company(xml_path=xml_path, ico=args.ico, record_tag=args.record_tag, replace=args.replace)
    if ok:
        print("✅ Import hotový.")
    else:
        print("⚠️ IČO nebylo v tomto dumpu nalezeno.")


if __name__ == "__main__":
    main()
