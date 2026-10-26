-- A selected article belongs to the earliest run. Accelerate cross-run
-- anti-join checks. Without this index thousands of candidates can scan all
-- previous runs repeatedly and exhaust Cloudflare D1 Free row-read quota.
CREATE INDEX IF NOT EXISTS wl_candidate_prior_by_article
 ON wl_candidates(evidence_key,run_key,selected);


