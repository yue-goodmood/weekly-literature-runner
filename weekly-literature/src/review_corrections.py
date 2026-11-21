"""Append source-verified private review amendments without editing initial decisions."""
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit


def apply_correction(store, run, item):
    key = item['evidence_key']
    analysis = item['analysis']
    source = item['source_url']
    title = item['source_title']
    reason = item['reason']
    if (urlsplit(source).scheme != 'https' or not urlsplit(source).hostname or
            not title.strip() or not reason.strip() or analysis['decision'] not in ('include', 'exclude') or
            not analysis['summary'].strip() or not analysis['popscience'].strip() or
            analysis['popscience_priority'] not in ('high', 'medium', 'low')):
        raise ValueError('Invalid correction')
    if analysis['popscience_priority'] == 'high':
        draft = analysis.get('popscience_article') or {}
        if analysis['decision'] != 'include' or not all(draft.get(k) for k in ('title', 'body', 'references')):
            raise ValueError('Missing patient education draft')
    payload = json.dumps(analysis, ensure_ascii=False, sort_keys=True)
    original = item['original_payload']
    if json.loads(original).get('decision') != 'exclude':
        raise ValueError('This review path corrects previously excluded records')
    store.sql("""INSERT OR IGNORE INTO wl_review_corrections
        (run_key,evidence_key,original_payload,payload,source_url,source_title,reason)
        SELECT a.run_key,a.evidence_key,a.payload,?,?,?,? FROM wl_analysis a
        JOIN wl_candidates c USING(run_key,evidence_key) JOIN wl_runs r USING(run_key)
        JOIN wl_articles b USING(evidence_key)
        WHERE a.run_key=? AND a.evidence_key=? AND r.status='READY' AND c.selected=1
        AND a.payload=? AND json_extract(b.payload,'$.title')=?""",
        (payload, source, title, reason, run, key, original, title))
    result = store.sql("""SELECT original_payload,payload,source_url,source_title,reason
                          FROM wl_review_corrections WHERE run_key=? AND evidence_key=?""", (run,key))
    expected = dict(original_payload=original,payload=payload,source_url=source,source_title=title,reason=reason)
    if result != [expected]:
        raise ValueError('Correction conflict, source mismatch or run already frozen')


def main(path):
    from active_d1 import resolve_database
    from pipeline import Store
    packet = json.loads(Path(path).read_text())
    store = Store(database_id=resolve_database(packet['active_database']))
    for item in packet['corrections']:
        apply_correction(store, packet['run_key'], item)
    print('Verified corrections:', len(packet['corrections']))


if __name__ == '__main__':
    main(sys.argv[1])
