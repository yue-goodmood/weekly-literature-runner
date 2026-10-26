"""Explicit idempotent recovery of truncated batches; keep analysis and pages."""
import json
import os
from pipeline import Store, select_candidates, request, ROOT, ingest_articles
from report_format import _canonical, REPORT_STYLE_VERSION
import hashlib


BASE = 'recovery:unlimited-v1:publication-base'


def reopen(store, run):
    state = store.sql('SELECT * FROM wl_runs WHERE run_key=?', (run,))[0]
    if state['status'] not in ('READY', 'PUBLISHED'):
        raise ValueError('Recovery requires materialized READY or PUBLISHED run')
    audit = json.loads(state['audit'])
    # Snapshot the exact verified old report and selected keys before expansion.
    # Never erase publication hashes, existing decisions or old audit facts.
    if state['status'] == 'PUBLISHED' and not store.sql(
            'SELECT 1 FROM wl_checkpoints WHERE run_key=? AND unit=?', (run, BASE)):
        from verify_publication import verify
        artifacts = verify(store, run)
        report = next(x for x in artifacts if x['artifact'] == run)
        headers = {'Authorization':'Bearer '+os.environ['NOTION_TOKEN'], 'Notion-Version':'2025-09-03'}
        blocks, cursor = [], None
        while True:
            url = 'https://api.notion.com/v1/blocks/'+report['page_id']+'/children'
            if cursor:
                url += '?start_cursor='+cursor
            data = json.loads(request(url, headers=headers))
            blocks.extend(_canonical(b) for b in data['results'])
            if not data['has_more']:
                break
            cursor = data['next_cursor']
        actual = hashlib.sha256(json.dumps({'style':REPORT_STYLE_VERSION,'blocks':blocks},
            ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        if actual != report['content_hash']:
            raise ValueError('Report changed during recovery snapshot; preserve user edits')
        keys = [r['evidence_key'] for r in store.sql(
            'SELECT evidence_key FROM wl_candidates WHERE run_key=? AND selected=1 ORDER BY evidence_key', (run,))]
        snapshot = dict(blocks=blocks, evidence_keys=keys, audit=audit,
                        page_id=report['page_id'], content_hash=actual, config_hash=state['config_hash'])
        store.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)', (run, BASE, json.dumps(snapshot)))
    previous = 'recovery:unlimited-v1:original-audit'
    store.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)', (run, previous, state['audit']))
    counts = select_candidates(store, run, None)
    excluded_elsewhere = store.sql(
        'SELECT count(*) n FROM wl_candidates c WHERE c.run_key=? AND c.selected=0', (run,))[0]['n']
    analyzed = store.sql('SELECT count(*) n FROM wl_analysis a JOIN wl_candidates c '
        'ON a.run_key=c.run_key AND a.evidence_key=c.evidence_key WHERE c.run_key=? AND c.selected=1', (run,))[0]['n']
    audit.update(counts, deferred=0, reserved_or_historical=excluded_elsewhere,
                 review_policy='unlimited-v1', analyzed=analyzed,
                 recovery={'reason':'remove_upstream_100_candidate_limit','original_audit_checkpoint':previous})
    if state['status']=='PUBLISHED' and analyzed==counts['selected']:
        return dict(run_key=run, status='PUBLISHED', **counts, analyzed=analyzed, remaining=0)
    store.sql("UPDATE wl_runs SET status='READY',audit=? WHERE run_key=?", (json.dumps(audit), run))
    # Set membership readback, not just count arithmetic.
    if store.sql('SELECT 1 FROM wl_candidates c WHERE c.run_key=? AND c.selected=0 '
                 'AND NOT EXISTS (SELECT 1 FROM wl_history h WHERE h.evidence_key=c.evidence_key) '
                 'AND NOT EXISTS (SELECT 1 FROM wl_candidates old WHERE old.evidence_key=c.evidence_key '
                 'AND old.run_key<c.run_key AND old.selected=1) LIMIT 1', (run,)):
        raise ValueError('Recovery left eligible records unqueued')
    return dict(run_key=run, status='READY', **counts, analyzed=analyzed,
                remaining=counts['selected']-analyzed, reserved_or_historical=excluded_elsewhere)


def repair_supplements(store, run):
    from supplements import harvest, EuropePMC
    state = store.sql('SELECT * FROM wl_runs WHERE run_key=?', (run,))[0]
    if state['status'] != 'READY':
        raise ValueError('Open review queue before recovering supplementary pages')
    audit = json.loads(state['audit'])
    previous = audit['supplementary_sources']['Europe PMC']
    if previous['status'] == 'COMPLETE_FOR_QUERY':
        return previous
    settings = json.loads((ROOT/'config/topics.json').read_text())['supplementary_sources']['Europe PMC']
    start, end = previous['window_start'], previous['window_end']
    namespace_unit = 'recovery:europe-pmc:pagination-generation'
    rows = store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?', (run, namespace_unit))
    namespace = json.loads(rows[0]['payload']) if rows else ''
    def execute(prefix):
        return harvest(store, run, EuropePMC(request), settings['queries'], start, end,
                       None, None, ingest_many=lambda items: ingest_articles(store, run, items), namespace=prefix)
    try:
        try:
            result = execute(namespace)
        except ValueError as exc:
            # Old cached counts can change while a batch sits truncated. Only
            # one explicit new generation; never loop indefinitely or call a
            # mixed-count enumeration complete.
            stale_page = any(message in str(exc) for message in (
                'count changed', 'Repeated source record', 'repeated source cursor'))
            if namespace or not stale_page:
                raise
            namespace = 'recovery-v1:'
            store.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)', (run, namespace_unit, json.dumps(namespace)))
            result = execute(namespace)
    except (ValueError, RuntimeError, OSError) as exc:
        if isinstance(exc, RuntimeError) and exc.args == ('D1_DAILY_QUOTA_EXCEEDED',):
            raise
        result = dict(status='PARTIAL', window_start=start, window_end=end,
                      error=type(exc).__name__+': '+str(exc)[:500])
    audit['supplementary_sources']['Europe PMC'] = result
    store.sql('UPDATE wl_runs SET audit=? WHERE run_key=?', (json.dumps(audit), run))
    if result['status'] == 'COMPLETE_FOR_QUERY':
        # Acquisition is repaired; older reports still retain their original
        # audit. Newly discovered studies must finish review in this run.
        store.sql("UPDATE wl_source_gaps SET status='RECOVERED_PENDING_REVIEW' WHERE source=? AND window_start>=? AND window_end<=? AND status='PARTIAL'", ('Europe PMC',start,end))
    reopen(store, run)
    return result


if __name__ == '__main__':
    store, run = Store(), os.environ['RUN_KEY']
    store.migrate()
    opened = reopen(store, run)
    print(json.dumps(opened, ensure_ascii=False), flush=True)
    if opened['status'] == 'READY':
        source_result = repair_supplements(store, run)
        print(json.dumps(source_result, ensure_ascii=False), flush=True)
        print(json.dumps(reopen(store, run), ensure_ascii=False), flush=True)
        if source_result['status'] != 'COMPLETE_FOR_QUERY':
            raise SystemExit('Supplement recovery remains PARTIAL; audit and candidate progress were preserved')


