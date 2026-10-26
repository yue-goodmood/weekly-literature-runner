CREATE TABLE IF NOT EXISTS wl_source_gaps (
 source TEXT NOT NULL, run_key TEXT NOT NULL, window_start TEXT NOT NULL,
 window_end TEXT NOT NULL, status TEXT NOT NULL, error TEXT,
 PRIMARY KEY(source,run_key)
);
CREATE INDEX IF NOT EXISTS wl_open_source_gaps ON wl_source_gaps(source,status,window_start);
CREATE TABLE IF NOT EXISTS wl_imports (
 content_hash TEXT PRIMARY KEY, record_count INTEGER NOT NULL
);


