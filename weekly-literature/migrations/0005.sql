-- Cross-run immutable-evidence checks must not scan every saved analysis.
CREATE INDEX IF NOT EXISTS wl_analysis_by_evidence
 ON wl_analysis(evidence_key,run_key);
