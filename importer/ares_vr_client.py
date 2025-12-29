
import json
import time
import sqlite3
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

# ARES VR endpoint (VR = Výpis z OR v režimu rejstříku)
ARES_VR_URL = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty-vr/{ico}"


# ---------------------------
# Helpers
# ---------------------------

def norm_ico(s: str) -> str:
    """Normalizuje IČO na 8 číslic (přidá nulu, pokud má 7)."""
    digits = re.sub(r"\D+", "", s or "")
    if len(digits) == 7:
        digits = "0" + digits
    return digits


def ensure_ares_cache_schema(db_path: str) -> None:
    """
    Zajistí existenci cache tabulky pro ARES VR a MIGRUJE staré schéma na nové.
    Nové schéma:
        ares_vr_cache(ico TEXT PRIMARY KEY, fetched_at TEXT NOT NULL, payload_json TEXT NOT NULL)
    Staré schéma (historicky):
        - sloupec 'payload' (TEXT) místo 'payload_json'
        - sloupec 'updated_at' (TEXT) místo 'fetched_at'
    """
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()

        # 1) Vytvoř tabulku v novém schématu (idempotentně)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ares_vr_cache (
                ico TEXT PRIMARY KEY,
                fetched_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ares_vr_cache_fetched_at ON ares_vr_cache(fetched_at)")
        con.commit()

        # 2) Migrační krok – podle skutečných sloupců uprav staré názvy
        cols = {row[1] for row in cur.execute("PRAGMA table_info(ares_vr_cache)").fetchall()}

        # a) payload -> payload_json
        if "payload" in cols and "payload_json" not in cols:
            cur.execute("ALTER TABLE ares_vr_cache ADD COLUMN payload_json TEXT")
            cur.execute("UPDATE ares_vr_cache SET payload_json = payload WHERE payload_json IS NULL")
            con.commit()
            cols.add("payload_json")

        # b) updated_at -> fetched_at
        if "updated_at" in cols and "fetched_at" not in cols:
            cur.execute("ALTER TABLE ares_vr_cache ADD COLUMN fetched_at TEXT")
            cur.execute("UPDATE ares_vr_cache SET fetched_at = updated_at WHERE fetched_at IS NULL")
            con.commit()
            cols.add("fetched_at")

        # c) pokud fetched_at pořád chybí (extrémní edge case)
        if "fetched_at" not in cols:
            cur.execute("ALTER TABLE ares_vr_cache ADD COLUMN fetched_at TEXT")
            cur.execute("UPDATE ares_vr_cache SET fetched_at = COALESCE(fetched_at, DATETIME('now'))")
            con.commit()

        # d) zajisti index
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ares_vr_cache_fetched_at ON ares_vr_cache(fetched_at)")
        con.commit()


# ---------------------------
# Config
# ---------------------------

@dataclass
class AresClientConfig:
    timeout_s: int = 20
    max_retries: int = 4
    backoff_base_s: float = 0.7
    # jednoduchý rate limit
    min_delay_between_requests_s: float = 0.25


# ---------------------------
# Client
# ---------------------------

class AresVrClient:
    def __init__(self, db_path: str, cfg: Optional[AresClientConfig] = None):
        self.db_path = db_path
        self.cfg = cfg or AresClientConfig()
        self._last_request_ts = 0.0

        # 🔑 AUTOMATICKÁ MIGRACE + SCHÉMA
        ensure_ares_cache_schema(self.db_path)

    # ---- interní ----

    def _sleep_rate_limit(self):
        now = time.time()
        dt = now - self._last_request_ts
        if dt < self.cfg.min_delay_between_requests_s:
            time.sleep(self.cfg.min_delay_between_requests_s - dt)
        self._last_request_ts = time.time()

    # ---- veřejné API ----

    def get_vr(self, ico: str, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Vrátí JSON z ARES VR API pro dané IČO.
        Používá cache (SQLite), pokud není force_refresh=True.
        Při 400/404 uloží do cache záznam s _error, aby se zbytečně nerequestovalo.
        """
        ico = norm_ico(ico)

        if not force_refresh:
            cached = self._cache_get(ico)
            if cached is not None:
                return cached

        self._sleep_rate_limit()

        url = ARES_VR_URL.format(ico=ico)
        last_err: Optional[Exception] = None

        for attempt in range(self.cfg.max_retries + 1):
            try:
                r = requests.get(
                    url,
                    timeout=self.cfg.timeout_s,
                    headers={
                        "Accept": "application/json",
                        "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
                        "User-Agent": "MDG-UBO-Tool/1.0 (+https://streamlit.app)"
                    },
                )

                # OK
                if r.status_code == 200:
                    # pokus o JSON; pokud selže, zapiš text do _error
                    try:
                        payload = r.json()
                    except Exception:
                        payload = {
                            "_error": "Chyba parsování JSON z ARES VR",
                            "_url": url,
                            "_raw": r.text[:2000],
                        }
                    self._cache_put(ico, payload)
                    return payload

                # 400 / 404 → neexistuje / špatné IČO; ulož do cache a vrať
                if r.status_code in (400, 404):
                    payload = {
                        "_error": f"ARES HTTP {r.status_code}",
                        "_url": url,
                    }
                    self._cache_put(ico, payload)
                    return payload

                # 429 / 5xx → retry s backoff
                last_err = RuntimeError(f"ARES HTTP {r.status_code}: {r.text[:200]}")

            except Exception as e:
                last_err = e

            # exponenciální backoff
            sleep_s = self.cfg.backoff_base_s * (2 ** attempt)
            time.sleep(min(sleep_s, 6.0))

        # po vyčerpání pokusů
        raise RuntimeError(f"ARES request failed after retries: {last_err}")

    # ---- cache ----

    def _cache_get(self, ico: str) -> Optional[Dict[str, Any]]:
        """Vrátí payload (dict) z cache, nebo None, pokud není k dispozici."""
        with sqlite3.connect(self.db_path) as con:
            row = con.execute(
                "SELECT payload_json FROM ares_vr_cache WHERE ico=?",
                (ico,),
            ).fetchone()
            if not row:
                return None
            try:
                return json.loads(row[0])
            except Exception:
                # poškozená cache – raději ignoruj, ať se udělá fresh request
                return None

    def _cache_put(self, ico: str, payload: Dict[str, Any]) -> None:
        """Zapíše payload do cache s časem fetched_at (UTC ISO)."""
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with sqlite3.connect(self.db_path) as con:
            con.execute(
                """
                INSERT INTO ares_vr_cache(ico, fetched_at, payload_json)
                VALUES (?, ?, ?)
                ON CONFLICT(ico) DO UPDATE SET
                    fetched_at=excluded.fetched_at,
                    payload_json=excluded.payload_json
                """,
                (ico, now, json.dumps(payload, ensure_ascii=False)),
            )
            con.commit()
