"""Re-triage an existing READY queue without touching saved analyses.

This is intentionally bounded and resumable for the D1 free tier. Upstream
source checkpoints and article metadata stay intact; only unanalysed candidate
rows that fail the current deterministic policy are removed.
"""
import hashlib
import json
import os

from pipeline import Store, TRIAGE_POLICY, score


PAGE = 250
PROGRESS = 'triage:'+TRIAGE_POLICY+':progress'
COMPLETE = 'triage:'+TRIAGE_POLICY+':complete'


def _checkpoint(store, run, unit):
    rows = store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?', (run, unit))
    return json.loads(rows[0]['payload']) if rows else None


def _save_checkpoint(store, run, unit, payload):
    store.sql('INSERT INTO wl_checkpoints VALUES(?,?,?) '
              'ON CONFLICT(run_key,unit) DO UPDATE SET payload=excluded.payload',
              (run, unit, json.dumps(payload, sort_keys=True)))


def retune(store, run):
    state = store.sql('SELECT * FROM wl_runs WHERE run_key=?', (run,))
    if not state or state[0]['status'] != 'READY':
        raise ValueError('Queue retuning requires a READY run')
    completed = _checkpoint(store, run, COMPLETE)
    if completed:
        return completed

    progress = _checkpoint(store, run, PROGRESS)
    if not progress:
        baseline = store.sql('''
            SELECT count(*) AS candidates,
                   sum(CASE WHEN c.selected=1 AND n.evidence_key IS NULL THEN 1 ELSE 0 END) AS pending
            FROM wl_candidates c LEFT JOIN wl_analysis n
              ON n.run_key=c.run_key AND n.evidence_key=c.evidence_key
            WHERE c.run_key=?
        ''', (run,))[0]
        progress = {'after':'', 'examined':0, 'removed':0,
                    'initial_candidates':baseline['candidates'],
                    'initial_pending':baseline['pending'] or 0,
                    'removed_digest':hashlib.sha256().hexdigest()}
        # Persist the baseline before the first destructive page so a quota
        # interruption cannot make the final removal count ambiguous.
        _save_checkpoint(store, run, PROGRESS, progress)
    after = progress['after']
    while True:
        rows = store.sql('''
            SELECT c.evidence_key,a.payload
            FROM wl_candidates c JOIN wl_articles a USING(evidence_key)
            LEFT JOIN wl_analysis n ON n.run_key=c.run_key AND n.evidence_key=c.evidence_key
            WHERE c.run_key=? AND c.selected=1 AND n.evidence_key IS NULL
              AND c.evidence_key>?
            ORDER BY c.evidence_key LIMIT ?
        ''', (run, after, PAGE))
        if not rows:
            break
        rejected = [row['evidence_key'] for row in rows if score(json.loads(row['payload'])) == 0]
        for offset in range(0, len(rejected), 80):
            group = rejected[offset:offset+80]
            if not group:
                continue
            store.sql('DELETE FROM wl_candidates WHERE run_key=? AND evidence_key IN ('+
                      ','.join('?' for _ in group)+') AND evidence_key NOT IN '
                      '(SELECT evidence_key FROM wl_analysis WHERE run_key=?)',
                      (run, *group, run))
        digest_material = progress['removed_digest']+'\n'+''.join(key+'\n' for key in rejected)
        after = rows[-1]['evidence_key']
        progress = {
            **progress,
            'after':after,
            'examined':progress['examined']+len(rows),
            'removed':progress['removed']+len(rejected),
            'removed_digest':hashlib.sha256(digest_material.encode()).hexdigest(),
        }
        _save_checkpoint(store, run, PROGRESS, progress)

    counts = store.sql('SELECT count(*) AS eligible,coalesce(sum(selected),0) AS selected '
                       'FROM wl_candidates WHERE run_key=?', (run,))[0]
    analyzed = store.sql('SELECT count(*) AS n FROM wl_analysis a JOIN wl_candidates c '
        'ON c.run_key=a.run_key AND c.evidence_key=a.evidence_key '
        'WHERE c.run_key=? AND c.selected=1', (run,))[0]['n']
    removed = progress['initial_candidates']-counts['eligible']
    audit = json.loads(state[0]['audit'])
    audit.update(counts, analyzed=analyzed, deferred=0,
                 reserved_or_historical=counts['eligible']-counts['selected'],
                 review_policy='clinical-signal-unlimited-v1',
                 triage={'policy':TRIAGE_POLICY, 'examined':progress['initial_pending'],
                         'removed':removed,
                         'removed_digest':progress['removed_digest'],
                         'preserved_analyses':analyzed})
    store.sql('UPDATE wl_runs SET audit=? WHERE run_key=?', (json.dumps(audit), run))
    result = dict(run_key=run, policy=TRIAGE_POLICY, examined=progress['initial_pending'],
                  removed=removed, selected=counts['selected'], analyzed=analyzed,
                  pending=counts['selected']-analyzed,
                  removed_digest=progress['removed_digest'])
    _save_checkpoint(store, run, COMPLETE, result)
    return result


if __name__ == '__main__':
    print(json.dumps(retune(Store(), os.environ['RUN_KEY']), ensure_ascii=False))


