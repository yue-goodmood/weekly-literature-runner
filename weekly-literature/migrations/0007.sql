CREATE TABLE IF NOT EXISTS wl_review_corrections (
  run_key TEXT NOT NULL,
  evidence_key TEXT NOT NULL,
  original_payload TEXT NOT NULL,
  payload TEXT NOT NULL,
  source_url TEXT NOT NULL,
  source_title TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_key,evidence_key)
);

CREATE VIEW IF NOT EXISTS wl_effective_analysis AS
SELECT a.run_key,a.evidence_key,coalesce(c.payload,a.payload) AS payload,
       c.reason AS correction_reason,c.source_url AS correction_source
FROM wl_analysis a LEFT JOIN wl_review_corrections c
  ON a.run_key=c.run_key AND a.evidence_key=c.evidence_key;
