"""Deterministic ETL. Standard library only; no paid inference or Worker compute."""
import argparse
import contextlib
import contextvars
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import unicodedata
import urllib.request
import urllib.parse
import urllib.error
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
TRIAGE_POLICY = 'clinical-signal-v1'
DEADLINE = contextvars.ContextVar('literature_deadline', default=None)

class WorkBudgetReached(Exception):
    """Pause before another remote call; persisted checkpoints remain authoritative."""

class UpstreamDeferred(Exception):
    """A long server cooldown belongs to the next scheduled recovery, not CI sleep."""

@contextlib.contextmanager
def work_budget(seconds):
    if not 1 <= seconds <= 1200:
        raise ValueError('Invalid work budget')
    token = DEADLINE.set(time.monotonic() + seconds)
    try:
        yield
    finally:
        DEADLINE.reset(token)

def check_budget(wait=0):
    deadline = DEADLINE.get()
    if deadline is not None and time.monotonic() + wait + 65 >= deadline:
        raise WorkBudgetReached()

def retry_delay(headers, attempt):
    value = (headers or {}).get('Retry-After', '0')
    try:
        delay = float(value)
    except (ValueError, TypeError):
        try:
            delay = (parsedate_to_datetime(value) - dt.datetime.now(dt.timezone.utc)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            delay = 0
    delay = max(2 ** attempt, delay)
    if delay > 60:
        raise UpstreamDeferred()
    check_budget(delay)
    return delay

AI_TERMS = re.compile(
    r'artificial intelligence|machine learning|deep learning|radiomics|large language models?|'
    r'generative ai|foundation models?|ai agents?|neural networks?|transformers?|'
    r'natural language processing|computer[- ]aided|federated learning', re.I)
MEDICAL_TITLE_TERMS = re.compile(
    r'medical|clinical|health(?:care)?|patients?|diagnos|prognos|screen|triage|treatment|therapy|'
    r'surgery|surgical|oncolog|cancer|tumou?r|stroke|cardiac|heart|brain|breast|lung|liver|'
    r'kidney|renal|diabetes|autism|mental health|radiolog|patholog|histolog|imaging|'
    r'\b(?:ct|mri|ecg|eeg|ehr|emr)\b|ultrasound|endoscop|genom|drug|pharma', re.I)
CLINICAL_EVIDENCE_TERMS = re.compile(
    r'patients?|participants?|subjects?|cohorts?|multicent|multi-center|single-center|hospital|'
    r'clinicians?|physicians?|nurses?|prospective|retrospective|randomi[sz]|clinical trials?|'
    r'external validation|independent (?:test|validation|cohort)|real[- ]world|registr(?:y|ies)|'
    r'electronic health records?|medical records?|reader stud|human evaluation|expert review|'
    r'systematic reviews?|meta-analys|guidelines?|consensus|mimic-(?:iii|iv)|claims data', re.I)
COLORECTAL_TERMS = re.compile(
    r'colorectal|rectal (?:cancer|tumou?r|neoplasm|surgery|resection)|rectum|colectomy|proctectomy|'
    r'colon (?:cancer|tumou?r|neoplasm|surgery|resection)|low anterior resection|'
    r'total mesorectal|natural orifice specimen|\bnoses\b|low anterior resection syndrome|'
    r'结直肠|直肠|结肠', re.I)

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

def d1_daily_quota(value):
    return bool(re.search(r'free tier daily row|daily.*limit|quota.*exceed', str(value), re.I))

def request(url, data=None, headers=None):
    for attempt in range(3):
        check_budget()
        try:
            req = urllib.request.Request(url, data=data, headers=headers or {})
            with urllib.request.urlopen(req, timeout=60) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            parsed = urllib.parse.urlparse(url)
            if parsed.hostname == 'api.cloudflare.com' and '/d1/database/' in parsed.path:
                # D1 can report its daily quota as a non-2xx HTTP response.
                # Never retry this response even when the status is 429/5xx.
                detail = exc.read(16384).decode('utf-8', errors='replace')
                if d1_daily_quota(detail):
                    raise RuntimeError('D1_DAILY_QUOTA_EXCEEDED') from None
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 2:
                raise
            time.sleep(retry_delay(exc.headers, attempt))
        except (urllib.error.URLError, TimeoutError):
            if attempt == 2:
                raise
            check_budget(2 ** attempt)
            time.sleep(2 ** attempt)

class Store:
    def __init__(self, local=None):
        self.quota_exhausted = False
        self.db = sqlite3.connect(local) if local else None
        if self.db:
            self.db.row_factory = sqlite3.Row
        else:
            self.url = 'https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query'.format(
                os.environ['CLOUDFLARE_ACCOUNT_ID'], os.environ['LITERATURE_D1_ID'])
            self.headers = {'Authorization': 'Bearer ' + os.environ['CLOUDFLARE_API_TOKEN'], 'Content-Type': 'application/json'}

    def remote(self, body):
        if self.quota_exhausted:
            raise RuntimeError('D1_DAILY_QUOTA_EXCEEDED')
        try:
            result = json.loads(request(self.url, json.dumps(body).encode(), self.headers))
            details = result.get('errors') or [r.get('error') for r in result.get('result', [])]
            if d1_daily_quota(json.dumps(details)):
                raise RuntimeError('D1_DAILY_QUOTA_EXCEEDED')
            return result
        except RuntimeError as exc:
            if exc.args == ('D1_DAILY_QUOTA_EXCEEDED',):
                self.quota_exhausted = True
            raise

    def sql(self, sql, params=()):
        if self.db:
            cur = self.db.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
            self.db.commit()
            return rows
        result = self.remote({'sql': sql, 'params': list(params)})
        if not result.get('success') or not all(r.get('success') for r in result.get('result', [])):
            detail = json.dumps(result.get('errors') or [r.get('error') for r in result.get('result', [])],
                                ensure_ascii=False)[:500]
            if re.search(r'free tier daily row|daily.*limit|quota.*exceed', detail, re.I):
                raise RuntimeError('D1_DAILY_QUOTA_EXCEEDED')
            raise RuntimeError('D1 query failed: '+detail)
        return result['result'][0]['results']

    def batch(self, statements):
        # Callers provide only replay-safe writes. Checkpoints follow the full page.
        for offset in range(0, len(statements), 50):
            group = statements[offset:offset+50]
            if self.db:
                with self.db:
                    for sql, params in group:
                        self.db.execute(sql, params)
            else:
                body = {'batch': [{'sql': sql, 'params': list(params)} for sql, params in group]}
                result = self.remote(body)
                if not result.get('success') or len(result.get('result', [])) != len(group) or not all(r.get('success') for r in result.get('result', [])):
                    detail = json.dumps(result.get('errors') or [r.get('error') for r in result.get('result', [])],
                                        ensure_ascii=False)[:500]
                    if re.search(r'free tier daily row|daily.*limit|quota.*exceed', detail, re.I):
                        raise RuntimeError('D1_DAILY_QUOTA_EXCEEDED')
                    raise RuntimeError('D1 batch failed; page remains recoverable: '+detail)

    def migrate(self):
        # One cheap indexed manifest lookup replaces repeated DDL on every tick.
        migrations = sorted((ROOT / 'migrations').glob('*.sql'))
        manifest = digest([(m.name, m.read_text()) for m in migrations])
        exists = self.sql("SELECT 1 FROM sqlite_master WHERE type='table' AND name='wl_checkpoints'")
        if exists and self.sql('SELECT 1 FROM wl_checkpoints WHERE run_key=? AND unit=?',
                               ('system:schema', manifest)):
            return
        for migration in migrations:
            for statement in migration.read_text().split(';'):
                if statement.strip():
                    self.sql(statement)
        self.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',
                 ('system:schema', manifest, 'true'))

    def checkpoint(self, run, unit, fetch):
        rows = self.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?', (run, unit))
        if rows:
            return json.loads(rows[0]['payload'])
        value = fetch()
        self.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES (?,?,?)', (run, unit, json.dumps(value)))
        return value

def aliases(article):
    keys = []
    if article.get('pmid'):
        keys.append('PMID:' + article['pmid'])
    doi = re.sub(r'^(https?://(dx\.)?doi.org/|doi:\s*)', '', article.get('doi', '').strip(), flags=re.I).lower().rstrip('.,;')
    if doi:
        keys.append('DOI:' + doi)
    if article.get('arxiv_id'):
        keys.append('ARXIV:' + re.sub(r'v[0-9]+$', '', article['arxiv_id']))
    title = ''.join(c for c in unicodedata.normalize('NFKC', article['title']).casefold() if c.isalnum())
    if title:
        keys.append('TITLE:' + title)
    if not keys:
        raise ValueError('Missing article identifiers')
    return keys

def safe_metadata(article):
    """Europe PMC PPR abstracts can belong to another paper. Fail closed.

    Keep identifiers/title for discovery; raw source pages stay in checkpoints.
    A preprint needs original-source verification before its abstract is evidence.
    """
    result = dict(article)
    if result.get('source') == 'Europe PMC' and result.get('source_id', '').startswith('PPR:'):
        result['abstract'] = ''
        result['abstract_status'] = 'UNVERIFIED_UPSTREAM_PPR'
    return result

def authoritative_upgrade(existing, incoming):
    incoming = safe_metadata(incoming)
    if not existing:
        return incoming
    existing = safe_metadata(existing)
    canonical = bool(incoming.get('pmid')) and (not incoming.get('source') or
        incoming.get('source') == 'PubMed' or incoming.get('source_id', '').startswith('MED:'))
    shared = set(aliases(existing)) & set(aliases(incoming))
    if existing.get('abstract_status') == 'UNVERIFIED_UPSTREAM_PPR' and canonical and \
            any(x.startswith('DOI:') for x in shared) and any(x.startswith('TITLE:') for x in shared):
        return incoming
    return existing

def score(article):
    article = safe_metadata(article)
    title = article.get('title', '')
    text = title + ' ' + article.get('abstract', '')
    colorectal = bool(COLORECTAL_TERMS.search(text))
    if 'Retracted Publication' in article.get('types', []):
        return 0
    clinical_ai = (bool(AI_TERMS.search(title)) and
                   bool(MEDICAL_TITLE_TERMS.search(title)) and
                   bool(CLINICAL_EVIDENCE_TERMS.search(text)))
    if not (colorectal or clinical_ai):
        return 0
    return ((50 if colorectal else 30) +
            10 * bool(re.search(r'randomized|systematic review|meta-analys|guideline|consensus|'
                                r'external validation|shared decision|organ preservation|'
                                r'low anterior resection syndrome|natural orifice', text, re.I)) +
            5 * bool(article.get('abstract')))

class PubMed:
    def call(self, endpoint, **params):
        time.sleep(.36)  # below NCBI's unauthenticated request rate
        params.update(tool='weekly_literature', email=os.environ.get('NCBI_EMAIL', ''))
        return request('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/' + endpoint + '?' + urllib.parse.urlencode(params))

    def search(self, query, start, end, field):
        for attempt in range(5):
            response = json.loads(self.call('esearch.fcgi', db='pubmed', term=query, datetype=field,
                                         mindate=start, maxdate=end, retmax=9999, retmode='json'))
            result = response.get('esearchresult', {})
            if 'count' in result and 'idlist' in result:
                break
            if attempt == 4:
                raise RuntimeError('PubMed search returned an error payload: ' + str(response)[:500])
            time.sleep(2 ** attempt)
        count = int(result['count'])
        if count > 9999:
            raise OverflowError('Split date interval before retrieving >9999 PubMed IDs')
        ids = result['idlist']
        if len(set(ids)) != count:
            raise ValueError('Incomplete PMID set')
        return ids

    def records(self, ids):
        root = ET.fromstring(self.call('efetch.fcgi', db='pubmed', id=','.join(ids), retmode='xml'))
        records = []
        for node in root.findall('PubmedArticle') + root.findall('PubmedBookArticle'):
            text = lambda p: ''.join(node.find(p).itertext()) if node.find(p) is not None else ''
            records.append({'pmid': text('MedlineCitation/PMID') or text('BookDocument/PMID'), 'title': text('.//ArticleTitle'),
                'abstract': '\n'.join(''.join(x.itertext()) for x in node.findall('.//Abstract/AbstractText')),
                'doi': next((x.text or '' for x in node.findall('.//ArticleId') if x.get('IdType') == 'doi'), ''),
                'journal': text('.//Journal/Title') or text('.//BookTitle'),
                'publication_date': text('.//JournalIssue/PubDate') or text('.//Book/PubDate'),
                'types': [''.join(x.itertext()) for x in node.findall('.//PublicationType')]})
        if set(ids) != {a['pmid'] for a in records}:
            raise ValueError('Missing metadata; batch remains recoverable: '+','.join(sorted(set(ids)-{a['pmid'] for a in records})[:10]))
        return records

def search_split(store, source, run, name, query, start, end, field):
    unit = f'ids:{name}:{field}:{start}:{end}'
    def fetch():
        try:
            return source.search(query, start, end, field)
        except OverflowError:
            left, right = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
            if left == right:
                raise RuntimeError('Single day exceeds PubMed cap; refine query, never truncate')
            middle = left + (right-left)//2
            return sorted(set(search_split(store, source, run, name, query, start, middle.isoformat(), field) +
                              search_split(store, source, run, name, query, (middle+dt.timedelta(days=1)).isoformat(), end, field)))
    return store.checkpoint(run, unit, fetch)

def ingest_articles(store, key, articles):
    # Raw upstream pages remain checkpointed, but low-value records do not
    # consume D1 article, alias, or candidate rows.
    articles = [article for article in map(safe_metadata, articles) if score(article)]
    if not articles:
        return
    # Resolve a page together to avoid a network round trip for every alias.
    names = sorted({name for article in articles for name in aliases(article)})
    known = {}
    for offset in range(0, len(names), 90):
        group = names[offset:offset+90]
        rows = store.sql('SELECT alias,evidence_key FROM wl_aliases WHERE alias IN (' + ','.join('?' for _ in group) + ')', group)
        known.update((row['alias'], row['evidence_key']) for row in rows)
    statements = []
    payloads = {}
    evidence_names = sorted(set(known.values()))
    for offset in range(0, len(evidence_names), 90):
        group = evidence_names[offset:offset+90]
        payloads.update((r['evidence_key'], json.loads(r['payload'])) for r in store.sql(
            'SELECT evidence_key,payload FROM wl_articles WHERE evidence_key IN (' + ','.join('?' for _ in group) + ')', group))
    for article in articles:
        article_names = aliases(article)
        matches = {known[name] for name in article_names if name in known}
        strong = {known[name] for name in article_names if name in known and not name.startswith('TITLE:')}
        if len(strong) > 1:
            raise ValueError('Conflicting identifier aliases require reconciliation: '+json.dumps(
                {'source_id':article.get('source_id'),'keys':[name for name in article_names if not name.startswith('TITLE:')],
                 'matches':sorted(strong)},ensure_ascii=False))
        if strong:
            # Exact PMID/DOI/arXiv identity outranks a non-unique generic title.
            evidence = next(iter(strong))
        elif len(matches) == 1:
            candidate = next(iter(matches))
            existing_keys = set(aliases(payloads[candidate]))
            incoming_keys = set(article_names)
            incompatible = any(
                {k for k in existing_keys if k.startswith(kind)} and
                {k for k in incoming_keys if k.startswith(kind)} and
                not {k for k in existing_keys & incoming_keys if k.startswith(kind)}
                for kind in ('PMID:', 'DOI:', 'ARXIV:'))
            evidence = article_names[0] if incompatible else candidate
        else:
            evidence = article_names[0]
        existing = payloads.get(evidence)
        resolved = authoritative_upgrade(existing, article)
        if existing and resolved != existing and store.sql('SELECT 1 FROM wl_analysis WHERE evidence_key=? LIMIT 1', (evidence,)):
            raise ValueError('Metadata already used in an analysis; explicit reconciliation required')
        payloads[evidence] = resolved
        if existing != resolved:
            statements.append(('INSERT INTO wl_articles VALUES(?,?) ON CONFLICT(evidence_key) DO UPDATE SET payload=excluded.payload', (evidence, json.dumps(resolved))))
        for alias in article_names:
            if alias in known:
                continue
            known[alias] = evidence
            statements.append(('INSERT OR IGNORE INTO wl_aliases VALUES(?,?)', (alias, evidence)))
        value = score(resolved)
        if value:
            statements.append(('INSERT INTO wl_candidates SELECT ?,?,?,0 WHERE NOT EXISTS (SELECT 1 FROM wl_history WHERE evidence_key=?) ON CONFLICT(run_key,evidence_key) DO UPDATE SET score=excluded.score WHERE NOT EXISTS (SELECT 1 FROM wl_analysis WHERE run_key=? AND evidence_key=?)', (key,evidence,value,evidence,key,evidence)))
    store.batch(statements)


def ingest_article(store, key, article):
    ingest_articles(store, key, [article])


def run_supplements(store, key, config, run, adapters):
    from supplements import harvest
    audit = {}
    for name, settings in config['supplementary_sources'].items():
        if not isinstance(settings, dict) or not settings.get('enabled') or name not in adapters:
            audit[name] = {'status': 'NOT_ATTEMPTED'}
            continue
        gaps = store.sql("SELECT min(window_start) AS start FROM wl_source_gaps WHERE source=? AND status='PARTIAL'", (name,))
        start = min(run['window_start'], gaps[0]['start'] or run['window_start'])
        end = run['window_end']
        try:
            audit[name] = harvest(store, key, adapters[name], settings['queries'], start, end,
                                  lambda article: ingest_article(store, key, article), settings['max_pages'],
                                  ingest_many=lambda articles: ingest_articles(store, key, articles))
            store.sql("UPDATE wl_source_gaps SET status='CLOSED' WHERE source=? AND window_start>=? AND window_end<=?", (name,start,end))
        except (ValueError, RuntimeError, urllib.error.URLError, TimeoutError, ET.ParseError) as exc:
            if isinstance(exc, RuntimeError) and exc.args == ('D1_DAILY_QUOTA_EXCEEDED',):
                raise  # No audit write, no next source, no retry until reset.
            detail = type(exc).__name__+': '+str(exc)[:500]
            audit[name] = {'status':'PARTIAL', 'window_start':start, 'window_end':end, 'error':detail}
            store.sql("INSERT INTO wl_source_gaps VALUES(?,?,?,?,?,?) ON CONFLICT(source,run_key) DO UPDATE SET status=excluded.status,error=excluded.error", (name,key,start,end,'PARTIAL',detail))
    return audit


def run_etl(store, source, config, run_date, supplements=None):
    config_hash = digest(config)
    key = 'weekly-literature:' + run_date + ':Asia/Shanghai'
    end = dt.date.fromisoformat(run_date) - dt.timedelta(days=1)
    start = end - dt.timedelta(days=config['lookback_days']-1)
    # Never block a new weekly ETL because an older batch awaits AI or Notion.
    # Cross-run candidate selection below reserves the older batch's chosen articles.
    previous = store.sql('SELECT window_start,window_end,status FROM wl_runs WHERE run_key<? ORDER BY run_key DESC', (key,))
    if previous:
        start = min(start, dt.date.fromisoformat(previous[0]['window_end']) + dt.timedelta(days=1))
        for row in previous:
            if row['status'] == 'ETL':
                start = min(start, dt.date.fromisoformat(row['window_start']))
    store.sql('INSERT OR IGNORE INTO wl_runs(run_key,config_hash,window_start,window_end) VALUES(?,?,?,?)',
              (key, config_hash, start.isoformat(), end.isoformat()))
    run = store.sql('SELECT * FROM wl_runs WHERE run_key=?', (key,))[0]
    # Policy changes must not break already-materialized ETL's no-op reruns.
    # Unfinished ETL still requires the exact frozen configuration.
    if run['status'] != 'ETL':
        return key
    if run['config_hash'] != config_hash:
        raise ValueError('Frozen config differs; restore original config to resume')
    store.checkpoint(key, 'etl:frozen-config:v1', lambda: config)
    pmids = set()
    raw_hits = 0
    for name, query in config['queries'].items():
        for field in ('edat', 'crdt'):
            ids = search_split(store, source, key, name, query, run['window_start'], run['window_end'], field)
            raw_hits += len(ids)
            pmids.update(ids)
    ids = sorted(pmids)
    for offset in range(0, len(ids), 100):
        batch = ids[offset:offset+100]
        unit = 'ingested:' + digest(batch)
        if store.sql('SELECT 1 FROM wl_checkpoints WHERE run_key=? AND unit=?', (key,unit)):
            continue
        articles = store.checkpoint(key, 'metadata:' + digest(batch), lambda: source.records(batch))
        ingest_articles(store, key, articles)
        store.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)', (key,unit,'true'))
    supplementary_audit = run_supplements(store, key, config, run, supplements or {})
    # Carry unselected backlog only. Previously selected papers belong to their
    # original run and must not silently be counted in a newer weekly report.
    store.sql('INSERT OR IGNORE INTO wl_candidates SELECT ?,c.evidence_key,max(c.score),0 FROM wl_candidates c LEFT JOIN wl_history h USING(evidence_key) WHERE c.run_key<? AND c.selected=0 AND h.evidence_key IS NULL GROUP BY c.evidence_key', (key,key))
    counts = select_candidates(store, key, config['max_candidates'])
    audit = dict(raw_hits=raw_hits, unique_pmids=len(ids), metadata_success=len(ids), **counts,
                 deferred=counts['eligible']-counts['selected'], supplementary_sources=supplementary_audit)
    if config['max_candidates'] is None:
        audit['review_policy'] = 'clinical-signal-unlimited-v1'
        audit['triage'] = {'policy': TRIAGE_POLICY}
        audit['reserved_or_historical'] = audit['deferred']
        audit['deferred'] = 0
    store.sql("UPDATE wl_runs SET status='READY',audit=? WHERE run_key=?", (json.dumps(audit), key))
    return key

def select_candidates(store, key, cap):
    """Fill this week's budget without stealing older runs' selected papers."""
    if cap is None:
        # Unlimited review still uses ten-item MCP pages and durable decisions.
        # Preserve existing selections, including previously excluded papers.
        store.sql('UPDATE wl_candidates AS c SET selected=1 WHERE run_key=? AND selected=0 '
                  'AND NOT EXISTS (SELECT 1 FROM wl_candidates old WHERE old.evidence_key=c.evidence_key '
                  'AND old.run_key<c.run_key AND old.selected=1) '
                  'AND NOT EXISTS (SELECT 1 FROM wl_history h WHERE h.evidence_key=c.evidence_key)', (key,))
        return store.sql('SELECT count(*) AS eligible,coalesce(sum(selected),0) AS selected FROM wl_candidates WHERE run_key=?', (key,))[0]
    # Colorectal starts at 50 and clinically evaluated medical-AI below 50.
    ai_cap = int(cap * .4)
    store.sql('UPDATE wl_candidates SET selected=0 WHERE run_key=?', (key,))
    # An earlier batch owns a selected article until reviewed/published. Exclude
    # it from later selections even if the search-window overlap finds it again.
    available = ("NOT EXISTS (SELECT 1 FROM wl_candidates old "
                 "WHERE old.evidence_key=c.evidence_key "
                 "AND old.run_key<c.run_key AND old.selected=1) "
                 "AND NOT EXISTS (SELECT 1 FROM wl_history h WHERE h.evidence_key=c.evidence_key)")
    for condition, budget in [('c.score>=50', cap-ai_cap), ('c.score<50', ai_cap)]:
        store.sql('UPDATE wl_candidates SET selected=1 WHERE run_key=? AND evidence_key IN '
                  '(SELECT c.evidence_key FROM wl_candidates c WHERE c.run_key=? AND '+condition+
                  ' AND '+available+' ORDER BY c.score DESC,c.evidence_key LIMIT ?)', (key,key,budget))
    selected = store.sql('SELECT count(*) AS n FROM wl_candidates WHERE run_key=? AND selected=1', (key,))[0]['n']
    store.sql('UPDATE wl_candidates SET selected=1 WHERE run_key=? AND evidence_key IN '
              '(SELECT c.evidence_key FROM wl_candidates c WHERE c.run_key=? AND c.selected=0 '
              'AND '+available+' ORDER BY c.score DESC,c.evidence_key LIMIT ?)', (key,key,cap-selected))
    return store.sql('SELECT count(*) AS eligible,coalesce(sum(selected),0) AS selected FROM wl_candidates WHERE run_key=?', (key,))[0]

def reconcile_history(store, config, apply=False):
    """Remove imported repeats from untouched READY runs; never rewrite analyses."""
    plan=[]
    for run in store.sql("SELECT * FROM wl_runs WHERE status='READY' ORDER BY run_key"):
        key=run['run_key']
        n=store.sql('SELECT count(*) AS n FROM wl_candidates c JOIN wl_history h USING(evidence_key) WHERE c.run_key=?',(key,))[0]['n']
        if not n:continue
        if run['config_hash'] != digest(config):
            raise ValueError('Restore frozen config before reconciling READY candidates')
        if store.sql('SELECT 1 FROM wl_analysis WHERE run_key=? LIMIT 1',(key,)):
            raise ValueError('READY run already has analyses; reconcile imported history manually')
        plan.append((run,n))
    if apply:
        for run,n in plan:
            key=run['run_key']
            # Delete only unprocessed duplicates; source checkpoints and articles remain.
            store.sql('DELETE FROM wl_candidates WHERE run_key=? AND evidence_key IN (SELECT evidence_key FROM wl_history)',(key,))
            counts=select_candidates(store,key,config['max_candidates'])
            audit=json.loads(run['audit'])
            audit.update(counts,deferred=counts['eligible']-counts['selected'],history_excluded=audit.get('history_excluded',0)+n)
            store.sql('UPDATE wl_runs SET audit=? WHERE run_key=?',(json.dumps(audit),key))
    return {'applied':apply,'runs':[{'run_key':r['run_key'],'duplicates':n} for r,n in plan]}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--local')
    parser.add_argument('--date')
    parser.add_argument('--migrate', action='store_true')
    args = parser.parse_args()
    store = Store(args.local)
    if args.migrate:
        store.migrate()
        return
    if not args.date:
        # Weekly cadence is Monday Shanghai time; preserve the same run key
        # on manual/backfill reruns later in the week.
        today = dt.datetime.now(ZoneInfo('Asia/Shanghai')).date()
        args.date = (today - dt.timedelta(days=today.weekday())).isoformat()
    from supplements import EuropePMC, Arxiv
    print(run_etl(store, PubMed(), json.loads((ROOT/'config/topics.json').read_text()), args.date,
                  {'Europe PMC': EuropePMC(request), 'arXiv': Arxiv(request)}))

if __name__ == '__main__':
    main()




