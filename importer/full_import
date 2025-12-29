import argparse
import sqlite3
from pathlib import Path
from typing import Optional

# bereme existující parsery a DB helpery z import_or.py
from importer.import_or import (
    BASE_DIR,
    DB_PATH,
    SCHEMA_PATH,
    init_db,
    iter_records,
    extract_company_ico_and_name,
    extract_partners_from_subjekt,
    get_or_create_entity_company,
    get_or_create_entity_person,
    insert_edge,
    upsert_company,
)

def ensure_indexes(con: sqlite3.Connection):
    """
    Indexy výrazně zrychlí dotazy aplikace.
    Pokud už existují, SQLite je ignoruje.
    """
    con.execute("CREATE INDEX IF NOT EXISTS idx_edge_target ON ownership_edge(target_ico)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_entity_ico ON entity(ico)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_entity_type ON entity(type)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_company_name ON company(name)")


def truncate_all(con: sqlite3.Connection):
    """
    Když chceš čistý reimport.
    """
    con.execute("DELETE FROM ownership_edge")
    con.execute("DELETE FROM entity")
    con.execute("DELETE FROM company")


def full_import_one_dump(
    xml_path: Path,
    record_tag: str,
    commit_every: int,
    mode: str,
    limit: Optional[int] = None,
):
    xml_path = xml_path.expanduser().resolve()
    if not xml_path.exists():
        raise SystemExit(f"Soubor neexistuje: {xml_path}")

    init_db()

    with sqlite3.connect(DB_PATH) as con:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.row_factory = sqlite3.Row

        ensure_indexes(con)

        # režim: truncate jen jednou, když uživatel požádá
        if mode == "truncate":
            print("🧹 Truncating DB tables (company/entity/ownership_edge)…")
            truncate_all(con)
            con.commit()

        scanned = 0
        imported = 0
        edge_count = 0

        print(f"🚀 Full import: {xml_path.name}")

        cur = con.cursor()

        for subjekt in iter_records(xml_path, record_tag=record_tag):
            scanned += 1
            if limit and scanned > limit:
                break

            ico, name = extract_company_ico_and_name(subjekt)
            if not ico:
                continue

            # upsert firma
            upsert_company(con, ico, name or "")

            # partners / owners
            partners = extract_partners_from_subjekt(subjekt)

            # optional: když je v dumpu firma bez společníků/akcionářů, přeskočíme edges
            if partners:
                # v append módu necháváme existující; v replace módu smažeme hrany pro firmu
                if mode == "replace":
                    cur.execute("DELETE FROM ownership_edge WHERE target_ico=?", (ico,))

                for p in partners:
                    if p["kind"] == "COMPANY" and p["ico"]:
                        owner_id = get_or_create_entity_company(con, p["ico"], p["name"])
                    else:
                        owner_id = get_or_create_entity_person(con, p["name"])

                    # insert edge (kompatibilní s tvou verzí insert_edge)
                    insert_edge(
                        con,
                        target_ico=ico,
                        owner_entity_id=owner_id,
                        share_pct=p.get("share_pct"),
                        share_raw=p.get("share_raw"),
                    )
                    edge_count += 1

            imported += 1

            # commit po dávkách
            if scanned % commit_every == 0:
                con.commit()
                print(f"… {scanned:,} subjektů, {imported:,} firem, {edge_count:,} hran")

        con.commit()
        print("✅ Hotovo.")
        print(f"   Prohledáno subjektů: {scanned:,}")
        print(f"   Zpracováno firem:    {imported:,}")
        print(f"   Vloženo hran:        {edge_count:,}")
        print(f"   DB: {DB_PATH}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xml", required=True, help="Cesta k .xml nebo .xml.gz dumpu")
    ap.add_argument("--record-tag", default="Subjekt", help="Tag záznamu (default Subjekt)")
    ap.add_argument("--commit-every", type=int, default=2000, help="Commit po N subjektech (default 2000)")
    ap.add_argument(
        "--mode",
        choices=["append", "replace", "truncate"],
        default="append",
        help=(
            "append = přidává (rychlé, ale může duplikovat hrany při opakovaném běhu), "
            "replace = pro každou firmu smaže její hrany a vloží znovu, "
            "truncate = smaže CELOU DB a naimportuje čistě"
        ),
    )
    ap.add_argument("--limit", type=int, default=None, help="Pro test jen prvních N subjektů")
    args = ap.parse_args()

    full_import_one_dump(
        xml_path=Path(args.xml),
        record_tag=args.record_tag,
        commit_every=args.commit_every,
        mode=args.mode,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
