import json
import sqlite3
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "database.sqlite"
CONFIG_PATH = BASE_DIR / "config" / "dumps.json"
EXPORTS_DIR = BASE_DIR / "exports"

from importer.full_import import full_import_one_dump  # používáme existující full import
from importer.bulk_seed import read_clients_csv  # z bulk_seed.py
# export_subset_db má v ukázce svůj vlastní schema; použijeme ho jako funkci níže


ProgressCB = Optional[Callable[[str, float], None]]  # (message, progress 0..1)


def load_dump_config() -> Dict:
    if not CONFIG_PATH.exists():
        return {"dumps": []}
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def save_dump_config(cfg: Dict):
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_exports_dir():
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


def ensure_indexes(con: sqlite3.Connection):
    con.execute("CREATE INDEX IF NOT EXISTS idx_edge_target ON ownership_edge(target_ico)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_entity_ico ON entity(ico)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_entity_type ON entity(type)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_company_name ON company(name)")


def run_full_import_from_config(
    mode_first: str = "truncate",
    mode_next: str = "append",
    commit_every: int = 2000,
    limit: Optional[int] = None,
    progress: ProgressCB = None,
) -> Tuple[bool, str]:
    """
    Spustí full import pro všechny dumpy v configu.
    První dump: truncate (čistý import), další: append (naváže).
    """
    cfg = load_dump_config()
    dumps = cfg.get("dumps", [])
    if not dumps:
        return False, "V config/dumps.json nejsou žádné dumpy."

    # validace cest
    for d in dumps:
        p = BASE_DIR / d["path"]
        if not p.exists():
            return False, f"Chybí soubor dumpu: {d['label']} ({p})"

    for i, d in enumerate(dumps):
        mode = mode_first if i == 0 else mode_next
        msg = f"Full import: {d['label']} (mode={mode})"
        if progress:
            progress(msg, i / max(1, len(dumps)))

        full_import_one_dump(
            xml_path=(BASE_DIR / d["path"]),
            record_tag=d.get("record_tag", "Subjekt"),
            commit_every=commit_every,
            mode=mode,
            limit=limit,
        )

    # indexy po importu
    with sqlite3.connect(DB_PATH) as con:
        ensure_indexes(con)
        con.commit()

    if progress:
        progress("Hotovo: full import dokončen.", 1.0)

    return True, "Full import dokončen."


def collect_subgraph_company_icos(con: sqlite3.Connection, roots: List[str], max_depth: int) -> Tuple[List[str], List[str]]:
    """
    Vrátí (unique_company_icos, missing_company_icos).
    Bere jen firmy; osoby se vezmou přes hrany při exportu.
    """
    con.row_factory = sqlite3.Row

    def norm_ico(s: str) -> str:
        digits = "".join(ch for ch in s if ch.isdigit())
        return digits.zfill(8)

    def has_edges(ico: str) -> bool:
        r = con.execute("SELECT 1 FROM ownership_edge WHERE target_ico=? LIMIT 1", (ico,)).fetchone()
        return r is not None

    def owners_company_icos(ico: str) -> List[str]:
        rows = con.execute(
            """
            SELECT e.ico AS owner_ico
            FROM ownership_edge oe
            JOIN entity e ON e.entity_id = oe.owner_entity_id
            WHERE oe.target_ico = ? AND e.type='COMPANY' AND e.ico IS NOT NULL
            """,
            (ico,),
        ).fetchall()
        return [norm_ico(r["owner_ico"]) for r in rows if r["owner_ico"]]

    visited = set()
    companies = set()
    missing = set()

    def dfs(ico: str, depth: int):
        ico = norm_ico(ico)
        if ico in visited:
            return
        visited.add(ico)
        companies.add(ico)

        if depth >= max_depth:
            return

        if not has_edges(ico):
            missing.add(ico)
            return

        for child in owners_company_icos(ico):
            dfs(child, depth + 1)

    for r in roots:
        dfs(r, 0)

    return sorted(companies), sorted(missing)


def export_subset_db(company_icos: List[str], out_db_path: Path, progress: ProgressCB = None):
    """
    Vytvoří novou DB jen s:
      - company rows pro company_icos
      - ownership_edge pro target_ico v company_icos
      - entity pro owner_entity_id z těch hran
    """
    src_db = DB_PATH
    out_db_path.parent.mkdir(parents=True, exist_ok=True)
    if out_db_path.exists():
        out_db_path.unlink()

    with sqlite3.connect(src_db) as src, sqlite3.connect(out_db_path) as dst:
        src.row_factory = sqlite3.Row
        dst.row_factory = sqlite3.Row

        # minimal schema
        dst.executescript(
            """
            CREATE TABLE IF NOT EXISTS company (
              ico TEXT PRIMARY KEY,
              name TEXT
            );
            CREATE TABLE IF NOT EXISTS entity (
              entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
              type TEXT NOT NULL,
              ico TEXT,
              name TEXT
            );
            CREATE TABLE IF NOT EXISTS ownership_edge (
              edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
              target_ico TEXT NOT NULL,
              owner_entity_id INTEGER NOT NULL,
              share_num INTEGER,
              share_den INTEGER,
              share_pct REAL,
              share_raw TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_edge_target ON ownership_edge(target_ico);
            CREATE INDEX IF NOT EXISTS idx_entity_ico ON entity(ico);
            CREATE INDEX IF NOT EXISTS idx_entity_type ON entity(type);
            """
        )
        dst.commit()

        placeholders = ",".join(["?"] * len(company_icos))

        if progress:
            progress("Export: kopíruju company…", 0.1)

        companies = src.execute(
            f"SELECT ico, name FROM company WHERE ico IN ({placeholders})",
            company_icos,
        ).fetchall()
        dst.executemany("INSERT INTO company(ico, name) VALUES(?, ?)", [(r["ico"], r["name"]) for r in companies])
        dst.commit()

        if progress:
            progress("Export: kopíruju ownership_edge…", 0.4)

        edges = src.execute(
            f"""
            SELECT edge_id, target_ico, owner_entity_id, share_num, share_den, share_pct, share_raw
            FROM ownership_edge
            WHERE target_ico IN ({placeholders})
            """,
            company_icos,
        ).fetchall()
        dst.executemany(
            """
            INSERT INTO ownership_edge(edge_id, target_ico, owner_entity_id, share_num, share_den, share_pct, share_raw)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            [(e["edge_id"], e["target_ico"], e["owner_entity_id"], e["share_num"], e["share_den"], e["share_pct"], e["share_raw"]) for e in edges],
        )
        dst.commit()

        if progress:
            progress("Export: kopíruju entity…", 0.7)

        owner_ids = sorted({e["owner_entity_id"] for e in edges})
        if owner_ids:
            placeholders_ids = ",".join(["?"] * len(owner_ids))
            ents = src.execute(
                f"SELECT entity_id, type, ico, name FROM entity WHERE entity_id IN ({placeholders_ids})",
                owner_ids,
            ).fetchall()
            dst.executemany(
                "INSERT INTO entity(entity_id, type, ico, name) VALUES(?, ?, ?, ?)",
                [(r["entity_id"], r["type"], r["ico"], r["name"]) for r in ents],
            )
            dst.commit()

        if progress:
            progress("Export hotový.", 1.0)


def run_client_seed_and_export(
    clients_csv_path: Path,
    depth: int,
    out_db_path: Path,
    out_report_path: Path,
    progress: ProgressCB = None,
) -> Tuple[List[str], List[str], List[str]]:
    """
    1) Načte IČO klientů
    2) Nasbírá subgraf (company_icos) do hloubky depth
    3) Vytvoří report + export DB
    """
    ensure_exports_dir()

    clients = read_clients_csv(clients_csv_path)
    if not clients:
        raise SystemExit("clients.csv je prázdný nebo špatný formát.")

    with sqlite3.connect(DB_PATH) as con:
        if progress:
            progress("Seed: sbírám podgraf pro klienty…", 0.15)

        company_icos, missing = collect_subgraph_company_icos(con, clients, max_depth=depth)

    report_lines = []
    report_lines.append(f"Klientů: {len(clients)}")
    report_lines.append(f"Hloubka: {depth}")
    report_lines.append(f"Unikátních firem v podgrafu: {len(company_icos)}")
    report_lines.append(f"Firem bez hran (nelze dál rozkrýt): {len(missing)}")
    report_lines.append("")
    if missing:
        report_lines.append("CHYBI_DATA_PRO_ICO:")
        report_lines.extend(missing)
        report_lines.append("")
    report_lines.append("SEZNAM_FIRM_ICO:")
    report_lines.extend(company_icos)

    out_report_path.write_text("\n".join(report_lines), encoding="utf-8")

    if progress:
        progress("Export: vytvářím klientskou DB…", 0.35)

    export_subset_db(company_icos, out_db_path, progress=progress)

    return clients, company_icos, missing
