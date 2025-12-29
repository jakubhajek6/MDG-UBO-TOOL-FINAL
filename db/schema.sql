CREATE TABLE IF NOT EXISTS company (
  ico TEXT PRIMARY KEY,
  name TEXT
);

CREATE TABLE IF NOT EXISTS entity (
  entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,              -- PERSON / COMPANY
  ico TEXT,                        -- jen pro COMPANY
  name TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_company_ico
ON entity(type, ico)
WHERE type='COMPANY' AND ico IS NOT NULL;

CREATE TABLE IF NOT EXISTS ownership_edge (
  target_ico TEXT NOT NULL,        -- firma, kterou vlastní
  owner_entity_id INTEGER NOT NULL, -- kdo vlastní
  share_num INTEGER,               -- např. 1
  share_den INTEGER,               -- např. 3
  share_pct REAL,                  -- např. 50.0
  share_raw TEXT,                  -- původní text z OR
  FOREIGN KEY (target_ico) REFERENCES company(ico),
  FOREIGN KEY (owner_entity_id) REFERENCES entity(entity_id)
);

CREATE INDEX IF NOT EXISTS idx_edges_target ON ownership_edge(target_ico);

-- Cache ARES VR odpovědí (online režim)
CREATE TABLE IF NOT EXISTS ares_vr_cache (
  ico TEXT PRIMARY KEY,
  fetched_at TEXT NOT NULL,
  payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ares_vr_cache_fetched_at ON ares_vr_cache(fetched_at);
