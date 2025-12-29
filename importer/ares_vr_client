import json
import time
import sqlite3
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests


ARES_VR_URL = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty-vr/{ico}"


# ---------------------------
# Helpers
# ---------------------------

def norm_ico(s: str) -> str:
    digits = re.sub(r"\D+", "", s or "")
    if len(digits) == 7:
        digits = "0" + digits
    return digits


def ensure_ares_cache_schema(db_path: str) -> None:
    """
    Zajistí existenci cache tabulky pro ARES VR.
    Volá se automaticky při startu klienta.
    """
    with sqlite3.connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ares_vr_cache (
                ico TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_ares_vr_cache_fetched_at ON ares_vr_cache(fetched_at)"
        )
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

        # 🔑 AUTOMATICKÁ MIGRACE
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
        Vrátí JSON z ARES VR API.
        Používá cache (SQLite), pokud není force_refresh=True.
        """
        ico = norm_ico(ico)

        if not force_refresh:
            cached = self._cache_get(ico)
            if cached is not None:
                return cached

        self._sleep_rate_limit()

        url = ARES_VR_URL.format(ico=ico)
        last_err = None

        for attempt in range(self.cfg.max_retries + 1):
            try:
                r = requests.get(
                    url,
                    timeout=self.cfg.timeout_s,
                    headers={"Accept": "application/json"},
                )

                if r.status_code == 200:
                    payload = r.json()
                    self._cache_put(ico, payload)
                    return payload

                # 400 / 404 → neexistuje, uložíme do cache
                if r.status_code in (400, 404):
                    payload = {
                        "_error": f"ARES HTTP {r.status_code}",
                        "_url": url,
                    }
                    self._cache_put(ico, payload)
                    return payload

                # 429 / 5xx → retry
                last_err = RuntimeError(
                    f"ARES HTTP {r.status_code}: {r.text[:200]}"
                )

            except Exception as e:
                last_err = e

            sleep_s = self.cfg.backoff_base_s * (2 ** attempt)
            time.sleep(min(sleep_s, 6.0))

        raise RuntimeError(f"ARES request failed after retries: {last_err}")

    # ---- cache ----

    def _cache_get(self, ico: str) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as con:
            row = con.execute(
                "SELECT payload_json FROM ares_vr_cache WHERE ico=?",
                (ico,),
            ).fetchone()
            if not row:
                return None
            return json.loads(row[0])

    def _cache_put(self, ico: str, payload: Dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
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
