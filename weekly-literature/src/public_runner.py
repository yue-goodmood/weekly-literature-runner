"""Public CI entry point: no production payloads or exceptions in public logs."""
import argparse
import contextlib
import datetime as dt
import json
import os
import tempfile
import urllib.error
from zoneinfo import ZoneInfo

from pipeline import (ROOT, Store, PubMed, TRIAGE_POLICY, digest, request, run_etl,
                      work_budget, WorkBudgetReached, UpstreamDeferred, DatabaseBudgetReached)
from publish import publish
from partial_publish import sync_partial, finish_progress, has_unsynced
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


def execute(mode, store=None, today=None):
    if os.environ.get('ENABLE_PRODUCTION') != 'true':
        return 'DISABLED'
    needed = REQUIRED if mode in ('readiness', 'publish', 'scheduled') else REQUIRED[:3]
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
    # Repair oldest legacy queue BEFORE spending quota on a new retrieval run.
    for row in (store.sql("SELECT run_key,audit FROM wl_runs WHERE status='READY' ORDER BY run_key")
                if mode in ('etl','scheduled') else []):
        if not strict(json.loads(row['audit'])):
            retune(store, row['run_key'])
            return 'RETRIAGED'  # One bounded recovery task; no new ETL this pass.
    if mode in ('etl', 'scheduled'):
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
        rows = store.sql("""
            SELECT r.run_key,r.audit,r.status,
              (SELECT count(*) FROM wl_candidates c WHERE c.run_key=r.run_key AND c.selected=1) AS selected,
              (SELECT count(*) FROM wl_analysis a JOIN wl_candidates c
                 ON c.run_key=a.run_key AND c.evidence_key=a.evidence_key
                 WHERE c.run_key=r.run_key AND c.selected=1) AS analyzed
            FROM wl_runs r WHERE r.status IN ('READY','PUBLISHING') OR
              (r.status='PUBLISHED' AND EXISTS (SELECT 1 FROM wl_checkpoints p
                WHERE p.run_key=r.run_key AND p.unit='notion:partial:index:v1')
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
                from partial_publish import cached, INDEX
                from layout_upgrade import restyle_batch, upgrade_progress, adopt_title_translations
                entries = cached(store, row['run_key'], INDEX, [])
                if entries:
                    upgrade_progress(store, row['run_key'], entries)
                for entry in entries:
                    try:
                        adopt_title_translations(store, row['run_key'], entry)
                        restyle_batch(store, 'partial:'+row['run_key']+':'+str(entry['number']))
                    except Exception as exc:
                        raise PhaseError('PRESENTATION', exc) from None
                changed = False
                for _ in range(4):
                    if not sync_partial(store, row['run_key'], progress):
                        break
                    changed = True
                if row['selected'] == row['analyzed']:
                    if has_unsynced(store, row['run_key']):
                        return 'PARTIAL_SYNC_VERIFIED'
                    publish(store, row['run_key'])
                    finish_progress(store, row['run_key'])
                    return 'PUBLICATION_VERIFIED'
                return 'PARTIAL_SYNC_VERIFIED' if changed else 'AWAITING_REVIEW'
        return 'AWAITING_REVIEW' if rows else 'NO_PENDING_PUBLICATION'
    return 'ETL_CHECKPOINTED'


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
                   'NO_PENDING_ETL'}
        if status not in allowed:
            raise ConfigurationError()
        print('weekly-literature: '+status)
        return 0
    except BaseException as exc:
        cause = exc.cause if isinstance(exc, PhaseError) else exc
        if isinstance(cause, (WorkBudgetReached, UpstreamDeferred, DatabaseBudgetReached)):
            label = ('PAUSED_D1_RUN_BUDGET' if isinstance(cause, DatabaseBudgetReached) else
                     'PAUSED_WORK_BUDGET' if isinstance(cause, WorkBudgetReached) else
                     'PAUSED_UPSTREAM_COOLDOWN')
            print('weekly-literature: '+label)
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
            else:
                label = exc.phase+'_'+diagnostic(exc.cause)
        else:
            label = 'STOPPED_PRIVATE_DETAILS_SUPPRESSED'
        print('weekly-literature: '+label)
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=('readiness','etl','publish','scheduled'), default='readiness')
    raise SystemExit(safe_main(parser.parse_args().mode))


