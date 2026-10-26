"""Public CI entry point: no production payloads or exceptions in public logs."""
import argparse
import contextlib
import datetime as dt
import json
import os
import tempfile
from zoneinfo import ZoneInfo

from pipeline import ROOT, Store, PubMed, TRIAGE_POLICY, request, run_etl
from publish import publish
from retune_queue import retune
from supplements import EuropePMC, Arxiv


REQUIRED = ('CLOUDFLARE_API_TOKEN', 'CLOUDFLARE_ACCOUNT_ID', 'LITERATURE_D1_ID',
            'NOTION_TOKEN', 'NOTION_DATA_SOURCE_ID', 'NOTION_ARTICLE_DATA_SOURCE_ID')


class ConfigurationError(Exception):
    pass


def strict(audit):
    return (audit.get('review_policy') == 'clinical-signal-unlimited-v1' and
            audit.get('triage', {}).get('policy') == TRIAGE_POLICY)


def execute(mode, store=None, today=None):
    if os.environ.get('ENABLE_PRODUCTION') != 'true':
        return 'DISABLED'
    if any(not os.environ.get(key, '').strip() for key in REQUIRED):
        raise ConfigurationError()
    store = store if store is not None else Store()
    today = today or dt.datetime.now(ZoneInfo('Asia/Shanghai')).date()
    if mode == 'readiness':
        # Read-only; never migrate or mutate D1, and never emit selected content.
        store.sql('SELECT status FROM wl_runs ORDER BY run_key LIMIT 1')
        headers = {'Authorization': 'Bearer '+os.environ['NOTION_TOKEN'],
                   'Notion-Version': '2025-09-03'}
        for name in ('NOTION_DATA_SOURCE_ID', 'NOTION_ARTICLE_DATA_SOURCE_ID'):
            request('https://api.notion.com/v1/data_sources/'+os.environ[name], headers=headers)
        return 'READY'
    store.migrate()
    # Repair oldest legacy queue BEFORE spending quota on a new retrieval run.
    for row in store.sql("SELECT run_key,audit FROM wl_runs WHERE status='READY' ORDER BY run_key"):
        if not strict(json.loads(row['audit'])):
            retune(store, row['run_key'])
            return 'RETRIAGED'  # One bounded recovery task; no new ETL this pass.
    if mode in ('etl', 'scheduled') and (mode == 'etl' or today.weekday() in (0, 1)):
        config = json.loads((ROOT/'config/topics.json').read_text())
        date = (today-dt.timedelta(days=today.weekday())).isoformat()
        run_etl(store, PubMed(), config, date,
                {'Europe PMC': EuropePMC(request), 'arXiv': Arxiv(request)})
    if mode in ('publish', 'scheduled'):
        rows = store.sql("""
            SELECT r.run_key,r.audit,
              (SELECT count(*) FROM wl_candidates c WHERE c.run_key=r.run_key AND c.selected=1) AS selected,
              (SELECT count(*) FROM wl_analysis a JOIN wl_candidates c
                 ON c.run_key=a.run_key AND c.evidence_key=a.evidence_key
                 WHERE c.run_key=r.run_key AND c.selected=1) AS analyzed
            FROM wl_runs r WHERE r.status IN ('READY','PUBLISHING') ORDER BY r.run_key
        """)
        for row in rows:
            if strict(json.loads(row['audit'])) and row['selected'] == row['analyzed']:
                publish(store, row['run_key'])
                return 'PUBLICATION_VERIFIED'
        return 'AWAITING_REVIEW' if rows else 'NO_PENDING_PUBLICATION'
    return 'ETL_CHECKPOINTED'


def safe_main(mode, worker=execute):
    # Never tee, upload, or preserve private logs as CI artifacts. A TemporaryFile
    # bounds memory and is automatically unlinked, including on ordinary errors.
    try:
        with tempfile.TemporaryFile(mode='w+', encoding='utf-8') as private_log:
            with contextlib.redirect_stdout(private_log), contextlib.redirect_stderr(private_log):
                status = worker(mode)
        allowed = {'DISABLED', 'READY', 'RETRIAGED', 'ETL_CHECKPOINTED',
                   'PUBLICATION_VERIFIED', 'AWAITING_REVIEW', 'NO_PENDING_PUBLICATION'}
        if status not in allowed:
            raise ConfigurationError()
        print('weekly-literature: '+status)
        return 0
    except BaseException as exc:
        # Never stringify an exception in public CI: it can contain URLs,
        # upstream payloads, private page IDs, SQL parameters, or bearer tokens.
        if isinstance(exc, KeyboardInterrupt):
            label = 'INTERRUPTED'
        elif isinstance(exc, ConfigurationError):
            label = 'CONFIGURATION_REQUIRED'
        elif isinstance(exc, RuntimeError) and exc.args == ('D1_DAILY_QUOTA_EXCEEDED',):
            label = 'D1_DAILY_QUOTA_EXCEEDED'
        else:
            label = 'STOPPED_PRIVATE_DETAILS_SUPPRESSED'
        print('weekly-literature: '+label)
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=('readiness','etl','publish','scheduled'), default='readiness')
    raise SystemExit(safe_main(parser.parse_args().mode))

