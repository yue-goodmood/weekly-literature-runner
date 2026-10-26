-- Avoid sorting/scanning the entire enlarged selection on every ten-item page.
CREATE INDEX IF NOT EXISTS wl_candidate_evidence_page
 ON wl_candidates(run_key,selected,evidence_key);


