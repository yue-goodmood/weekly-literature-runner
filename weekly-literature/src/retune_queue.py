"""Resumable disease-scope review; preserve all source and analysis records."""
import hashlib
import json
import os
from pipeline import Store, TRIAGE_POLICY, score

PAGE = 24  # At most 49 statements: one atomic Store.batch transaction.
PROGRESS = 'triage:'+TRIAGE_POLICY+':progress'
COMPLETE = 'triage:'+TRIAGE_POLICY+':complete'
EXCLUDED = 'scope:'+TRIAGE_POLICY+':excluded:'


def _checkpoint(store, run, unit):
    rows=store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?',(run,unit))
    return json.loads(rows[0]['payload']) if rows else None


def _save_checkpoint(store, run, unit, payload):
    store.sql('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',(run,unit,json.dumps(payload,sort_keys=True)))


def retune(store, run):
    state=store.sql('SELECT * FROM wl_runs WHERE run_key=?',(run,))
    if not state or state[0]['status']!='READY':
        raise ValueError('Queue retuning requires a READY run')
    completed=_checkpoint(store,run,COMPLETE)
    if completed: return completed
    progress=_checkpoint(store,run,PROGRESS)
    if not progress:
        store.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',
            (run,'scope:'+TRIAGE_POLICY+':original-audit',state[0]['audit']))
        progress={'after':'','examined':0,'removed':0,'removed_analyzed':0,
                  'removed_digest':hashlib.sha256().hexdigest()}
        _save_checkpoint(store,run,PROGRESS,progress)
    while True:
        rows=store.sql('SELECT c.evidence_key,a.payload,n.evidence_key AS analyzed FROM wl_candidates c '
            'JOIN wl_articles a USING(evidence_key) LEFT JOIN wl_analysis n '
            'ON n.run_key=c.run_key AND n.evidence_key=c.evidence_key '
            'WHERE c.run_key=? AND c.selected=1 AND c.evidence_key>? '
            'ORDER BY c.evidence_key LIMIT ?',(run,progress['after'],PAGE))
        if not rows: break
        rejected=[r for r in rows if score(json.loads(r['payload']))==0]
        material=progress['removed_digest']+'\n'+''.join(r['evidence_key']+'\n' for r in rejected)
        next_progress={**progress,'after':rows[-1]['evidence_key'],
            'examined':progress['examined']+len(rows),'removed':progress['removed']+len(rejected),
            'removed_analyzed':progress['removed_analyzed']+sum(bool(r['analyzed']) for r in rejected),
            'removed_digest':hashlib.sha256(material.encode()).hexdigest()}
        statements=[]
        for row in rejected:
            statements.extend([
                ('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',(run,EXCLUDED+row['evidence_key'],
                    json.dumps({'policy':TRIAGE_POLICY,'reason':'outside_digestive_pelvic_scope',
                                'previous_selected':1,'analysis_preserved':bool(row['analyzed'])}))),
                ('UPDATE wl_candidates SET selected=0 WHERE run_key=? AND evidence_key=?',(run,row['evidence_key']))])
        statements.append(('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',
                           (run,PROGRESS,json.dumps(next_progress,sort_keys=True))))
        # Selection change, audit receipts and cursor commit atomically.
        store.batch(statements)
        progress=next_progress
    counts=store.sql('SELECT count(*) AS n FROM wl_candidates WHERE run_key=? AND selected=1',(run,))[0]
    analyzed=store.sql('SELECT count(*) AS n FROM wl_analysis a JOIN wl_candidates c '
        'ON c.run_key=a.run_key AND c.evidence_key=a.evidence_key '
        'WHERE c.run_key=? AND c.selected=1',(run,))[0]['n']
    audit=json.loads(state[0]['audit'])
    audit.update(selected=counts['n'],analyzed=analyzed,deferred=0,
        review_policy='clinical-signal-unlimited-v1',
        scope={'policy':TRIAGE_POLICY,'coverage':'legacy_queue_retriage_only',
               'expanded_retrieval':'next_new_run','historical_records_preserved':True},
        triage={'policy':TRIAGE_POLICY,'examined':progress['examined'],'removed':progress['removed'],
                'removed_analyzed':progress['removed_analyzed'],
                'removed_digest':progress['removed_digest'],'preserved_analyses':analyzed})
    result=dict(run_key=run,policy=TRIAGE_POLICY,examined=progress['examined'],
        removed=progress['removed'],removed_analyzed=progress['removed_analyzed'],
        selected=counts['n'],analyzed=analyzed,pending=counts['n']-analyzed)
    store.batch([
        ('UPDATE wl_runs SET audit=? WHERE run_key=?',(json.dumps(audit),run)),
        ('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',(run,COMPLETE,json.dumps(result)))])
    return result


if __name__=='__main__':
    print(json.dumps(retune(Store(),os.environ['RUN_KEY']),ensure_ascii=False))



