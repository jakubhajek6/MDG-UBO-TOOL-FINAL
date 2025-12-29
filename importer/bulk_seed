import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Set, Dict, Tuple, List, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "database.sqlite"


def norm_ico(s: str) -> str:
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits.zfill(8)


def db_connect(path: Path):
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def company_has_edges(con: sqlite3.Connection, ico: str) -> bool:
    row = con.execute("SELECT 1 FROM ownership_edge WHERE target_ico=? LIMIT 1", (ico,)).fetchone()
    return row is not None


def get_company_name(con: sqlite3.Connection, ico: str) -> Optional[str]:
    row = con.execute("SELECT name FROM company WHERE ico=?", (ico,)).fetchone()
    return row["name"] if row and row["name"] else None


def get_owners(con: sqlite3.Connection, ico: str) -> List[Dict]:
    rows = con.execute(
        """
        SELECT e.type AS owner_type, e.ico AS owner_ico, e.name AS owner_name
        FROM ownership_edge oe
        JOIN entity e ON e.entity_id = oe.owner_entity_id
        WHERE oe.target_ico = ?
        """,
        (ico,),
    ).fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "type": r["owner_type"],          # PERSON / COMPANY
                "ico": r["owner_ico"],            # pro COMPANY
                "name": r["owner_name"],
            }
        )
    return out


def collect_subgraph_for_company(
    con: sqlite3.Connection,
    root_ico: str,
    max_depth: int,
) -> Tuple[Set[str], Set[int], Set[str]]:
    """
    Vrátí:
      - companies_icos: všechny firmy (IČO) v grafu
      - missing_companies: firmy, které v DB nemají hrany (tj. neumíme rozkrýt)
      - roots: kořenové IČO (jen pro report)
    Pozn.: entity_id osob sbírat nemusíme zde, export je vybere podle hran.
    """
    root_ico = norm_ico(root_ico)
    companies: Set[str] = set([root_ico])
    missing: Set[str] = set()
    visited: Set[str] = set()

    def dfs_company(ico: str, depth: int):
        ico = norm_ico(ico)
        if ico in visited:
            return
        visited.add(ico)

        if depth > max_depth:
            return

        if not company_has_edges(con, ico):
            missing.add(ico)
            return

        for o in get_owners(con, ico):
            if o["type"] == "COMPANY" and o["ico"]:
                child = norm_ico(o["ico"])
                companies.add(child)
                dfs_company(child, depth + 1)

    dfs_company(root_ico, 0)
    return companies, set(), missing


def read_clients_csv(path: Path) -> List[str]:
    text = path.read_text(encoding="utf-8").splitlines()
    if not text:
        return []
    # zkus CSV s hlavičkou, jinak fallback "jeden řádek = IČO"
    if "," in text[0].lower() or "ico" in text[0].lower():
        out = []
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                val = row.get("ico") or row.get("IČO") or row.get("ICO")
                if val:
                    out.append(norm_ico(val))
        return out
    else:
        return [norm_ico(x) for x in text if x.strip()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", required=True, help="Cesta k clients.csv (sloupec ico nebo 1 IČO na řádek)")
    ap.add_argument("--max-depth", type=int, default=20, help="Hloubka rozkrývání do grafu (default 20)")
    ap.add_argument("--out", default="seed_result.txt", help="Výstupní report (default seed_result.txt)")
    args = ap.parse_args()

    clients_path = Path(args.clients).expanduser().resolve()
    if not clients_path.exists():
        raise SystemExit(f"Soubor neexistuje: {clients_path}")

    clients = read_clients_csv(clients_path)
    if not clients:
        raise SystemExit("clients.csv je prázdný nebo špatný formát")

    with db_connect(DB_PATH) as con:
        all_companies: Set[str] = set()
        all_missing: Set[str] = set()

        lines = []
        lines.append(f"Klientů: {len(clients)}")
        lines.append(f"Max depth: {args.max_depth}")
        lines.append("")

        for i, ico in enumerate(clients, start=1):
            name = get_company_name(con, ico) or "(bez názvu)"
            companies, _, missing = collect_subgraph_for_company(con, ico, max_depth=args.max_depth)

            all_companies |= companies
            all_missing |= missing

            lines.append(f"[{i}/{len(clients)}] {ico} {name}")
            lines.append(f"  firmy v grafu: {len(companies)}")
            if missing:
                lines.append(f"  ⚠️ chybí data pro: {len(missing)} (např. {', '.join(sorted(missing)[:8])}{'…' if len(missing)>8 else ''})")
            lines.append("")

        lines.append("===== Souhrn =====")
        lines.append(f"Celkem unikátních firem v podgrafu: {len(all_companies)}")
        lines.append(f"Celkem firem s chybějícími hranami: {len(all_missing)}")
        lines.append("")
        lines.append("SEZNAM_FIRM_ICO:")
        for ico in sorted(all_companies):
            lines.append(ico)

        Path(args.out).write_text("\n".join(lines), encoding="utf-8")
        print(f"✅ Hotovo. Report: {args.out}")
        print(f"   Unikátních firem v podgrafu: {len(all_companies)}")
        if all_missing:
            print(f"   ⚠️ Firmy bez hran: {len(all_missing)} (viz report)")

if __name__ == "__main__":
    main()
