"""Public CI entry point: no production payloads or exceptions in public logs."""
import argparse
import contextlib
import datetime as dt
import json
import os
import tempfile
import urllib.error
import uuid
from zoneinfo import ZoneInfo

from pipeline import (ROOT, Store, PubMed, TRIAGE_POLICY, digest, request, run_etl,
                      work_budget, check_budget, WorkBudgetReached, UpstreamDeferred, DatabaseBudgetReached)
from publish import publish
from partial_publish import sync_partial, finish_progress, has_unsynced, INDEX
from retune_queue import retune
from supplements import EuropePMC, Arxiv


REQUIRED = ('CLOUDFLARE_API_TOKEN', 'CLOUDFLARE_ACCOUNT_ID', 'LITERATURE_D1_ID',
            'NOTION_TOKEN', 'NOTION_DATA_SOURCE_ID', 'NOTION_ARTICLE_DATA_SOURCE_ID')


class ConfigurationError(Exception):
    pass

class FrozenConfigError(Exception):
    pass

class PhaseError(Exception):
    def __init__(self, phase, cause):
        self.phase, self.cause = phase, cause


def diagnostic(cause):
    # Match fixed code-owned prefixes only; never expose upstream error text.
    import re
    if isinstance(cause, ValueError) and re.fullmatch(r'Non-title content changed; preserve page: \d+ (text|links|type|icon|suffix|translatedtitle|unmatchedtitle)',str(cause)):
        return 'DISPLAY_DIFFERENCE_'+str(cause).rsplit(': ',1)[1].upper().replace(' ','_')
    prefixes = (
        ('Conflicting identifier aliases', 'IDENTIFIER_CONFLICT'),
        ('Metadata already used in an analysis', 'METADATA_RECONCILIATION_REQUIRED'),
        ('Missing metadata;', 'METADATA_INCOMPLETE'),
        ('Incomplete PMID set', 'PUBMED_RESULT_INCOMPLETE'),
        ('PubMed search returned an error payload', 'PUBMED_ERROR_PAYLOAD'),
        ('Source count changed', 'SOURCE_COUNT_CHANGED'),
        ('Repeated source record', 'SOURCE_RECORD_REPEATED'),
        ('Incomplete or repeated source cursor', 'SOURCE_CURSOR_INVALID'),
        ('Frozen config differs', 'FROZEN_CONFIG_MISMATCH'),
        ('D1 query failed:', 'D1_QUERY_FAILED'),
        ('D1 batch failed;', 'D1_BATCH_FAILED'),
    )
    message = str(cause)
    for prefix, label in prefixes:
        if message.startswith(prefix):
            return label
    label = {ValueError:'VALIDATION_FAILED', KeyError:'RESPONSE_FIELD_MISSING',
             TimeoutError:'NETWORK_TIMEOUT'}.get(type(cause), 'FAILED_PRIVATE_DETAILS_SUPPRESSED')
    trace = cause.__traceback__
    while trace is not None:
        filename = trace.tb_frame.f_code.co_filename.rsplit('/',1)[-1]
        if filename in ('pipeline.py','supplements.py','publish.py','retune_queue.py','layout_upgrade.py'):
            location = filename.removesuffix('.py').upper()+'_L'+str(trace.tb_lineno)
        trace = trace.tb_next
    return label+('_'+location if 'location' in locals() else '')


def etl_target(store, today, config):
    unfinished = store.sql("SELECT run_key,config_hash FROM wl_runs WHERE status='ETL' ORDER BY run_key LIMIT 1")
    if not unfinished:
        return (today-dt.timedelta(days=today.weekday())).isoformat(), config
    run = unfinished[0]
    cached = store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?',
                       (run['run_key'], 'etl:frozen-config:v1'))
    frozen = json.loads(cached[0]['payload']) if cached else config
    if digest(frozen) != run['config_hash']:
        raise FrozenConfigError()
    return run['run_key'].split(':')[1], frozen


def strict(audit):
    return (audit.get('review_policy') == 'clinical-signal-unlimited-v1' and
            audit.get('triage', {}).get('policy') == TRIAGE_POLICY)


def reserve_retrieval(store, now=None):
    """Atomically reserve capacity, then settle measured usage, not run count.

    Retrieval owns at most 3M reads/60k writes per UTC day, leaving headroom
    against the shared Free allowance for analysis/delivery/other services.
    Abandoned leases and old three-slot receipts are charged conservatively.
    This is not an account-wide meter; single queries can still overshoot.
    """
    if getattr(store,'retrieval_lease',None): raise ConfigurationError()
    day=(now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc).date()
    key='system:retrieval-budget:'+day.isoformat()
    unit='usage:'+uuid.uuid4().hex
    # One statement provides cross-run atomic admission, not a read-then-write
    # race. Retain old slot receipts; unknown historical usage is never erased.
    rows=store.sql("""WITH used AS (
      SELECT coalesce(sum(CASE WHEN unit>='slot:' AND unit<'slot;' THEN
        coalesce(json_extract(payload,'$.read_budget'),250000)+100000
        ELSE json_extract(payload,'$.charged_reads') END),0) AS reads,
        coalesce(sum(CASE WHEN unit>='slot:' AND unit<'slot;' THEN
        coalesce(json_extract(payload,'$.write_budget'),10000)+
        coalesce(json_extract(payload,'$.write_headroom'),5000)
        ELSE json_extract(payload,'$.charged_writes') END),0) AS writes
      FROM wl_checkpoints WHERE run_key=? AND
        ((unit>='slot:' AND unit<'slot;') OR (unit>='usage:' AND unit<'usage;')))
      INSERT INTO wl_checkpoints SELECT ?,?,json_object('state','RESERVED',
        'charged_reads',min(350000,3000000-reads),
        'charged_writes',min(15000,60000-writes)) FROM used
      WHERE reads<=2995000 AND writes<=58000 RETURNING payload""",(key,key,unit))
    if not rows: return False
    receipt=json.loads(rows[0]['payload'])
    # Work stops BEFORE the full process allowance, reserving 100 rows for a
    # single bounded usage settlement. Never reset resource counters or widen
    # the original per-process ceiling to perform bookkeeping.
    limits=(min(store.row_read_budget,250000,receipt['charged_reads']-1000),
            min(store.row_write_budget,10000,receipt['charged_writes']-1000))
    store.accounting_limits=limits
    store.row_read_budget,store.row_write_budget=(max(1,n-100) for n in limits)
    store.retrieval_lease=(key,unit)
    store.retrieval_day=day.isoformat()
    return True


def settle_retrieval(store):
    lease=getattr(store,'retrieval_lease',None)
    if not lease or store.quota_exhausted or not store.usage_complete: return
    limits=store.accounting_limits
    if store.rows_read>=limits[0]-10 or store.rows_written>=limits[1]-10:
        return  # A large last query used the reserve; retain the full lease.
    check_budget()
    payload={'state':'SETTLED','measured_reads':store.rows_read,'measured_writes':store.rows_written,
             'charged_reads':store.rows_read+10,'charged_writes':store.rows_written+10,
             'accounting_headroom':10}
    store.settling_budget=True
    try:
        store.sql('UPDATE wl_checkpoints SET payload=? WHERE run_key=? AND unit=?',
                   (json.dumps(payload),*lease))
        store.retrieval_lease=None
    finally:
        store.settling_budget=False


def _execute(mode, store=None, today=None, _reserved=False):
    if os.environ.get('ENABLE_PRODUCTION') != 'true':
        return 'DISABLED'
    needed = REQUIRED if mode in ('readiness', 'publish', 'scheduled', 'recovery') else REQUIRED[:3]
    if any(not os.environ.get(key, '').strip() for key in needed):
        raise ConfigurationError()
    store = store if store is not None else Store()
    today = today or dt.datetime.now(ZoneInfo('Asia/Shanghai')).date()
    if mode == 'readiness':
        # Read-only; never migrate or mutate D1, and never emit selected content.
        try:
            store.sql('SELECT status FROM wl_runs ORDER BY run_key LIMIT 1')
        except Exception as exc:
            raise PhaseError('D1', exc) from None
        headers = {'Authorization': 'Bearer '+os.environ['NOTION_TOKEN'],
                   'Notion-Version': '2025-09-03'}
        try:
            for name in ('NOTION_DATA_SOURCE_ID', 'NOTION_ARTICLE_DATA_SOURCE_ID'):
                request('https://api.notion.com/v1/data_sources/'+os.environ[name], headers=headers)
        except Exception as exc:
            raise PhaseError('NOTION', exc) from None
        return 'READY'
    store.migrate()
    if mode in ('etl','scheduled','recovery'):
        # Recovery checks must not start a new week's retrieval. Only the
        # original ETL trigger (or explicit ETL request) may create a batch.
        if mode=='recovery':
            from backfill import needed as backfill_needed
            config=json.loads((ROOT/'config/topics.json').read_text())
            pending=store.sql("SELECT * FROM wl_runs WHERE status IN ('ETL','READY') ORDER BY run_key")
            if not any(row['status']=='ETL' or not strict(json.loads(row['audit'])) or
                       (config.get('backfill_existing_windows') and backfill_needed(row))
                       for row in pending):
                return execute('publish',store,today)
        if not _reserved and not reserve_retrieval(store):
            return 'PAUSED_RETRIEVAL_DAILY_BUDGET'
        if mode=='recovery':
            result=execute('etl',store,today,_reserved=True)
            # Backfill completion is not final publication; publication keeps
            # all coverage/medical-review/readback gates and the SAME budget.
            return execute('publish',store,today) if result=='BACKFILL_COMPLETED' else result
    # Repair oldest legacy queue BEFORE spending quota on a new retrieval run.
    for row in (store.sql("SELECT run_key,audit FROM wl_runs WHERE status='READY' ORDER BY run_key")
                if mode in ('etl','scheduled','publish') else []):
        if not strict(json.loads(row['audit'])):
            try:
                retune(store, row['run_key'])
            except Exception as exc:
                raise PhaseError('SCOPE',exc) from None
            if mode!='publish':
                return 'RETRIAGED'  # No new ETL during a recovery pass.
    if mode in ('etl', 'scheduled'):
        from backfill import needed, resume
        config=json.loads((ROOT/'config/topics.json').read_text())
        for state in store.sql("SELECT * FROM wl_runs WHERE status='READY' ORDER BY run_key"):
            if config.get('backfill_existing_windows') and needed(state):
                try:
                    resume(store,state['run_key'],config,PubMed(),
                           {'Europe PMC':EuropePMC(request),'arXiv':Arxiv(request)})
                except Exception as exc:
                    raise PhaseError('BACKFILL',exc) from None
                return 'BACKFILL_COMPLETED'
        config = json.loads((ROOT/'config/topics.json').read_text())
        date, config = etl_target(store, today, config)
        existing = store.sql('SELECT status FROM wl_runs WHERE run_key=?',
                             ('weekly-literature:'+date+':Asia/Shanghai',))
        if existing and existing[0]['status'] != 'ETL':
            if mode == 'etl':
                return 'NO_PENDING_ETL'
        else:
            try:
                run_etl(store, PubMed(), config, date,
                        {'Europe PMC': EuropePMC(request), 'arXiv': Arxiv(request)})
            except Exception as exc:
                raise PhaseError('ETL', exc) from None
    if mode in ('publish', 'scheduled'):
        from backfill import needed
        config=json.loads((ROOT/'config/topics.json').read_text())
        rows = store.sql(f"""
            SELECT r.run_key,r.audit,r.status,
              (SELECT count(*) FROM wl_candidates c WHERE c.run_key=r.run_key AND c.selected=1) AS selected,
              (SELECT count(*) FROM wl_analysis a JOIN wl_candidates c
                 ON c.run_key=a.run_key AND c.evidence_key=a.evidence_key
                 WHERE c.run_key=r.run_key AND c.selected=1) AS analyzed
            FROM wl_runs r WHERE r.status IN ('READY','PUBLISHING') OR
              (r.status='PUBLISHED' AND EXISTS (SELECT 1 FROM wl_checkpoints p
                WHERE p.run_key=r.run_key AND p.unit='{INDEX}')
               AND NOT EXISTS (SELECT 1 FROM wl_checkpoints p WHERE p.run_key=r.run_key
                AND p.unit='notion:partial:finished:v1')) ORDER BY r.run_key
        """)
        for row in rows:
            if strict(json.loads(row['audit'])):
                if row.get('status') in ('PUBLISHED','PUBLISHING'):
                    if row['status'] == 'PUBLISHING':
                        publish(store, row['run_key'])
                    finish_progress(store, row['run_key'])
                    return 'PUBLICATION_VERIFIED'
                progress = {k: row[k] for k in ('selected','analyzed')}
                from partial_publish import cached
                entries = cached(store, row['run_key'], INDEX, [])
                changed = False
                while True:
                    check_budget()
                    try:
                        synced=sync_partial(store, row['run_key'], progress)
                    except Exception as exc:
                        if isinstance(exc,urllib.error.HTTPError) and exc.code==400 and getattr(exc,'safe_service','')=='D1':
                            detail=getattr(exc,'private_detail',None)
                            if detail:
                                store.sql("UPDATE wl_runs SET audit=json_set(audit,'$.delivery_diagnostic',json(?)) WHERE run_key=?",
                                    (json.dumps(detail),row['run_key']))
                        raise PhaseError('PARTIAL',exc) from None
                    if not synced:
                        break
                    changed = True
                if changed:
                    store.sql("UPDATE wl_runs SET audit=json_remove(audit,'$.delivery_diagnostic') WHERE run_key=?",(row['run_key'],))
                # Publication is independent of retrieval: a source cooldown,
                # snapshot restart or retrieval budget must not prevent delivery
                # of already reviewed evidence. Only etl/scheduled resumes sources.
                # Never finalize while the approved scope backfill is pending.
                state=store.sql('SELECT * FROM wl_runs WHERE run_key=?',(row['run_key'],))[0]
                if config.get('backfill_existing_windows') and needed(state):
                    return 'PARTIAL_SYNC_VERIFIED' if changed else 'AWAITING_BACKFILL'
                if row['selected'] == row['analyzed']:
                    if has_unsynced(store, row['run_key']):
                        return 'PARTIAL_SYNC_VERIFIED'
                    publish(store, row['run_key'])
                    finish_progress(store, row['run_key'])
                    return 'PUBLICATION_VERIFIED'
                return 'PARTIAL_SYNC_VERIFIED' if changed else 'AWAITING_REVIEW'
        return 'AWAITING_REVIEW' if rows else 'NO_PENDING_PUBLICATION'
    return 'ETL_CHECKPOINTED'


def execute(mode, store=None, today=None, _reserved=False):
    if os.environ.get('ENABLE_PRODUCTION')!='true': return 'DISABLED'
    needed=REQUIRED if mode in ('readiness','publish','scheduled','recovery') else REQUIRED[:3]
    if any(not os.environ.get(key,'').strip() for key in needed): raise ConfigurationError()
    store=store if store is not None else Store()
    owner=not _reserved and not getattr(store,'retrieval_lease',None)
    quota=False
    try:
        return _execute(mode,store,today,_reserved)
    except BaseException as exc:
        cause=exc.cause if isinstance(exc,PhaseError) else exc
        quota=isinstance(cause,RuntimeError) and cause.args==('D1_DAILY_QUOTA_EXCEEDED',)
        if quota: store.quota_exhausted=True
        raise
    finally:
        if owner and not quota:
            try:
                settle_retrieval(store)
            except Exception:
                # Unknown/crashed accounting retains the original conservative
                # reservation. Never retry bookkeeping or mask quota exhaustion.
                if getattr(store,'quota_exhausted',False):
                    raise RuntimeError('D1_DAILY_QUOTA_EXCEEDED') from None


def safe_main(mode, worker=execute):
    # Never tee, upload, or preserve private logs as CI artifacts. A TemporaryFile
    # bounds memory and is automatically unlinked, including on ordinary errors.
    try:
        with work_budget(int(os.environ.get('WORK_BUDGET_SECONDS', '600'))), tempfile.TemporaryFile(mode='w+', encoding='utf-8') as private_log:
            with contextlib.redirect_stdout(private_log), contextlib.redirect_stderr(private_log):
                status = worker(mode)
        allowed = {'DISABLED', 'READY', 'RETRIAGED', 'ETL_CHECKPOINTED',
                   'PUBLICATION_VERIFIED', 'AWAITING_REVIEW', 'NO_PENDING_PUBLICATION',
                   'PARTIAL_SYNC_VERIFIED',
                   'NO_PENDING_ETL','BACKFILL_COMPLETED','AWAITING_BACKFILL',
                   'PAUSED_RETRIEVAL_DAILY_BUDGET'}
        if status not in allowed:
            raise ConfigurationError()
        print('weekly-literature: '+status)
        return 0
    except BaseException as exc:
        cause = exc.cause if isinstance(exc, PhaseError) else exc
        if isinstance(cause, (WorkBudgetReached, UpstreamDeferred, DatabaseBudgetReached)):
            from supplements import SourceSnapshotChanged
            label = ('PAUSED_SOURCE_SNAPSHOT_CHANGED' if isinstance(cause, SourceSnapshotChanged) else
                     'PAUSED_D1_RUN_BUDGET' if isinstance(cause, DatabaseBudgetReached) else
                     'PAUSED_WORK_BUDGET' if isinstance(cause, WorkBudgetReached) else
                     'PAUSED_UPSTREAM_COOLDOWN')
            print('weekly-literature: '+label)
            if isinstance(cause, DatabaseBudgetReached) and cause.usage:
                # Allowlisted resource counters only, never SQL, IDs or payloads.
                print('weekly-literature-usage: '+' '.join(
                    key+'='+str(int(cause.usage[key]))
                    for key in ('reads','writes','read_limit','write_limit')))
            return 0  # A checkpointed pause is not completion and needs no rerun storm.
        # Never stringify an exception in public CI: it can contain URLs,
        # upstream payloads, private page IDs, SQL parameters, or bearer tokens.
        if isinstance(exc, KeyboardInterrupt):
            label = 'INTERRUPTED'
        elif isinstance(exc, ConfigurationError):
            label = 'CONFIGURATION_REQUIRED'
        elif isinstance(exc, FrozenConfigError):
            label = 'FROZEN_CONFIG_REQUIRED'
        elif isinstance(exc, RuntimeError) and exc.args == ('D1_DAILY_QUOTA_EXCEEDED',):
            label = 'D1_DAILY_QUOTA_EXCEEDED'
        elif isinstance(exc, PhaseError):
            if isinstance(exc.cause, RuntimeError) and exc.cause.args == ('D1_DAILY_QUOTA_EXCEEDED',):
                label = 'D1_DAILY_QUOTA_EXCEEDED'
            elif isinstance(exc.cause, urllib.error.HTTPError):
                label = exc.phase+'_HTTP_'+str(exc.cause.code)
                import re
                field=getattr(exc.cause,'safe_field','')
                if re.fullmatch(r'body(?:\.[a-z_]+|\[\d+\])+',field): label+='_'+field
                service=getattr(exc.cause,'safe_service','')
                reason=getattr(exc.cause,'safe_reason','')
                if service in ('NOTION','D1','UPSTREAM'): label+='_'+service
                if reason in ('VALIDATION_ERROR','INVALID_JSON','INVALID_REQUEST_URL','MISSING_VERSION','TOO_MANY_SQL_VARIABLES'): label+='_'+reason
            else:
                label = exc.phase+'_'+diagnostic(exc.cause)
        else:
            label = 'STOPPED_PRIVATE_DETAILS_SUPPRESSED'
        print('weekly-literature: '+label)
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=('readiness','etl','publish','scheduled','recovery'), default='readiness')
    raise SystemExit(safe_main(parser.parse_args().mode))


