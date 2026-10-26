CREATE TABLE IF NOT EXISTS wl_runs (
 run_key TEXT PRIMARY KEY, config_hash TEXT NOT NULL, window_start TEXT NOT NULL,
 window_end TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'ETL', audit TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS wl_checkpoints (
 run_key TEXT NOT NULL, unit TEXT NOT NULL, payload TEXT NOT NULL,
 PRIMARY KEY(run_key,unit)
);
CREATE TABLE IF NOT EXISTS wl_articles (
 evidence_key TEXT PRIMARY KEY, payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS wl_aliases (
 alias TEXT PRIMARY KEY, evidence_key TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS wl_candidates (
 run_key TEXT NOT NULL, evidence_key TEXT NOT NULL, score INTEGER NOT NULL,
 selected INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(run_key,evidence_key)
);
CREATE INDEX IF NOT EXISTS wl_candidate_page ON wl_candidates(run_key,selected,score DESC,evidence_key);
CREATE TABLE IF NOT EXISTS wl_analysis (
 run_key TEXT NOT NULL, evidence_key TEXT NOT NULL, payload TEXT NOT NULL,
 PRIMARY KEY(run_key,evidence_key)
);
CREATE TABLE IF NOT EXISTS wl_publications (
 run_key TEXT PRIMARY KEY, page_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL DEFAULT 'PENDING', content_hash TEXT
);
CREATE TABLE IF NOT EXISTS wl_history (
 evidence_key TEXT PRIMARY KEY, run_key TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS wl_page_intents (
 run_key TEXT PRIMARY KEY
);


