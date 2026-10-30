-- Durable source identity ledger for direct-cursor resume, not full-page replay.
CREATE TABLE IF NOT EXISTS wl_harvest_seen (
 run_key TEXT NOT NULL,
 scan_key TEXT NOT NULL,
 value TEXT NOT NULL,
 PRIMARY KEY(run_key,scan_key,value)
);
