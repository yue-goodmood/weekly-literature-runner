"""Read-only archive access and resumable deduplication-index transfer."""
import json
import os
import re
import sys
import urllib.parse

from pipeline import Store, request
from public_runner import execute, safe_main, PhaseError

SYNC = 'system:active-d1-cutover:v1'
SEED_PAGE = 250
SOURCES = ('wl_history', 'wl_analysis')
UUID = re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', re.I)


def resolve_database(name):
    account = os.environ['CLOUDFLARE_ACCOUNT_ID']
    token = os.environ['CLOUDFLARE_API_TOKEN']
    base = f'https://api.cloudflare.com/client/v4/accounts/{account}/d1/database'
    query = urllib.parse.urlencode({'name': name, 'per_page': 100, 'page': 1})
    result = json.loads(request(base+'?'+query, headers={'Authorization': 'Bearer '+token}))
    rows = [row for row in result.get('result', []) if row.get('name') == name]
    if (result.get('success') is not True or len(rows) != 1
            or not UUID.fullmatch(rows[0].get('uuid', ''))):
        raise ValueError('Replacement database not verified')
    return rows[0]['uuid']


def seed_index(archive, active, max_pages=36):
    state = active.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?',
                       (SYNC, 'cursor'))
    cursor = json.loads(state[0]['payload']) if state else [0, '', '']
    for _ in range(max_pages):
        source, evidence, run = cursor
        if source == len(SOURCES):
            return True
        table = SOURCES[source]
        rows = archive.sql(f'SELECT evidence_key,run_key FROM {table} '
                           'WHERE (evidence_key,run_key)>(?,?) '
                           'ORDER BY evidence_key,run_key LIMIT ?',
                           (evidence, run, SEED_PAGE))
        if rows:
            cursor = [source, rows[-1]['evidence_key'], rows[-1]['run_key']]
        else:
            cursor = [source+1, '', '']
        writes = []
        if rows:
            writes.append(("INSERT OR IGNORE INTO wl_history(evidence_key,run_key) "
                           "SELECT json_extract(value,'$.evidence_key'),json_extract(value,'$.run_key') "
                           "FROM json_each(?)", (json.dumps(rows),)))
        writes.append(('INSERT INTO wl_checkpoints VALUES(?,?,?) '
                       'ON CONFLICT(run_key,unit) DO UPDATE SET payload=excluded.payload',
                       (SYNC, 'cursor', json.dumps(cursor))))
        active.batch(writes)
    return cursor[0] == len(SOURCES)


def run_active(mode):
    if os.environ.get('ENABLE_PRODUCTION') != 'true':
        return 'DISABLED'
    try:
        old_id = os.environ['LITERATURE_D1_ID']
        new_id = resolve_database(os.environ['LITERATURE_ACTIVE_D1_NAME'])
        if old_id.lower() == new_id.lower():
            raise ValueError('Archive and replacement must differ')
        archive = Store()
        active = Store(database_id=new_id)
        if mode == 'readiness':
            return execute(mode, active)
        active.migrate()
        if not seed_index(archive, active):
            return 'DEDUPE_SEED_CHECKPOINTED'
        # First ingest in the new database even when the deploy push asks for recovery.
        if mode in ('publish', 'recovery') and not active.sql('SELECT 1 FROM wl_runs LIMIT 1'):
            mode = 'etl'
    except Exception as exc:
        raise PhaseError('D1_CUTOVER', exc) from None
    return execute(mode, active)


if __name__ == '__main__':
    raise SystemExit(safe_main(sys.argv[1], run_active))
