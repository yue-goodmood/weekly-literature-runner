"""Public-source adapters. Every result page is independently resumable."""
import html
import datetime as dt
import json
import re
import time
import urllib.parse
import xml.etree.ElementTree as ET
from pipeline import UpstreamDeferred


class SourceSnapshotChanged(UpstreamDeferred):
    """Old pages are preserved; the next pass starts a separate coherent snapshot."""


def plain(value):
    return html.unescape(re.sub(r'<[^>]+>', ' ', value or '')).strip()


class EuropePMC:
    name = 'Europe PMC'
    checkpoint_namespace = 'epmc-date-v2:'
    supports_date_slicing = True

    def __init__(self, transport):
        self.transport = transport

    def page(self, query, start, end, cursor):
        params = {'query': f'({query}) AND FIRST_IDATE:[{start} TO {end}]',
                  'format': 'json', 'resultType': 'core', 'pageSize': 100,
                  'sort': 'P_PDATE_D asc',
                  'cursorMark': cursor or '*'}
        value = json.loads(self.transport('https://www.ebi.ac.uk/europepmc/webservices/rest/search?' + urllib.parse.urlencode(params)))
        if 'hitCount' not in value or 'resultList' not in value:
            raise ValueError('Invalid Europe PMC response')
        records = []
        for item in value['resultList'].get('result', []):
            source = item.get('source', '')
            source_id = item['id']
            types = item.get('pubTypeList', {}).get('pubType', [])
            records.append({'source': self.name, 'source_id': source+':'+source_id,
                'pmid': item.get('pmid', source_id if source == 'MED' else ''),
                'doi': item.get('doi', ''), 'title': plain(item.get('title')),
                'abstract': plain(item.get('abstractText')),
                'journal': item.get('journalInfo', {}).get('journal', {}).get('title', ''),
                'publication_date': item.get('firstPublicationDate', ''),
                'index_date': item.get('firstIndexDate', ''), 'types': types,
                'preprint': source == 'PPR' or any('preprint' in t.lower() for t in types),
                'url': 'https://europepmc.org/article/'+source+'/'+source_id})
        return {'records': records, 'total': int(value['hitCount']), 'next': value.get('nextCursorMark')}


class Arxiv:
    name = 'arXiv'
    supports_date_slicing = True

    def __init__(self, transport, pause=time.sleep):
        self.transport, self.pause = transport, pause

    def page(self, query, start, end, cursor):
        offset = int(cursor or 0)
        params = {'search_query': f'({query}) AND submittedDate:[{start.replace("-", "")}0000 TO {end.replace("-", "")}2359]',
                  'start': offset, 'max_results': 100, 'sortBy': 'submittedDate', 'sortOrder': 'ascending'}
        self.pause(3)
        root = ET.fromstring(self.transport('https://export.arxiv.org/api/query?' + urllib.parse.urlencode(params)))
        ns = {'a': 'http://www.w3.org/2005/Atom', 'o': 'http://a9.com/-/spec/opensearch/1.1/', 'x': 'http://arxiv.org/schemas/atom'}
        total = root.findtext('o:totalResults', namespaces=ns)
        if total is None:
            raise ValueError('Invalid arXiv response')
        records = []
        for item in root.findall('a:entry', ns):
            text = lambda path: ' '.join((item.findtext(path, default='', namespaces=ns)).split())
            identifier = text('a:id').split('/abs/')[-1]
            if not re.fullmatch(r'(\d{4}\.\d{4,5}|[a-z-]+(?:\.[A-Z]{2})?/\d{7})v\d+', identifier):
                raise ValueError('Invalid arXiv entry or upstream error')
            records.append({'source': self.name, 'source_id': identifier, 'arxiv_id': identifier,
                'title': text('a:title'), 'abstract': text('a:summary'), 'doi': text('x:doi'),
                'journal': text('x:journal_ref'), 'publication_date': text('a:published'),
                'updated_date': text('a:updated'), 'types': ['Preprint'], 'preprint': True,
                'url': 'https://arxiv.org/abs/'+identifier})
        following = offset + len(records)
        return {'records': records, 'total': int(total), 'next': str(following) if following < int(total) else None}


def harvest(store, run, adapter, queries, start, end, ingest, max_pages=None, ingest_many=None, namespace=''):
    """Freeze pages, verify unique source IDs/counts, never label capped queries complete."""
    counts = {}
    for name, query in queries.items():
        from pipeline import digest
        # Exact query/window/namespace identity: never reuse another week's coverage.
        completed_unit = namespace+getattr(adapter, 'checkpoint_namespace', '')+'supplement-complete:'+adapter.name+':'+digest([name,query,start,end,max_pages])
        complete = store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?',
                             (run,completed_unit))
        if complete:
            counts[name] = json.loads(complete[0]['payload'])['count']
            continue
        snapshot_unit = completed_unit+':snapshot'
        snapshots = store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?',
                              (run,snapshot_unit))
        generation = json.loads(snapshots[0]['payload'])['generation'] if snapshots else 0
        # Generation zero intentionally reuses existing checkpoints. Never edit or
        # delete frozen pages, canonical articles, or immutable medical reviews.
        page_prefix = '' if generation == 0 else 'snapshot:'+str(generation)+':'
        progress_unit = completed_unit+':progress:'+str(generation)
        saved = store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?',
                          (run,progress_unit))
        progress = json.loads(saved[0]['payload']) if saved else {}
        cursor = progress.get('cursor')
        expected = progress.get('expected')
        seen_count = progress.get('seen_count',0)
        page_number = progress.get('page_number',0)
        scan_key = digest([completed_unit,generation])
        # Old frozen pages are migrated once, without re-ingestion. Thereafter
        # the pointer and indexed identity ledger resume directly at the next page.
        while True:
            if max_pages is not None and page_number >= max_pages:
                raise ValueError('Source page budget reached; coverage remains partial')
            from pipeline import digest
            unit = namespace+getattr(adapter, 'checkpoint_namespace', '')+page_prefix+'supplement:'+adapter.name+':'+digest([name, query, start, end, cursor])
            page = store.checkpoint(run, unit, lambda: adapter.page(query, start, end, cursor))
            if expected is None:
                expected = page['total']
            if page['total'] != expected:
                # A live index may change even inside a fixed date window. Mixing
                # cached old pages with new pages can never establish coverage.
                # Retire only this query's snapshot and pause, without claiming
                # completion or repeatedly fetching within the same process.
                receipt = {'generation':generation+1, 'reason':'SOURCE_COUNT_CHANGED',
                           'previous_total':expected, 'observed_total':page['total']}
                store.sql('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',
                          (run,snapshot_unit,json.dumps(receipt)))
                raise SourceSnapshotChanged()
            identifiers = [article['source_id'] for article in page['records']]
            if len(identifiers) != len(set(identifiers)):
                raise ValueError('Repeated source record within page')
            following = page['next']
            count = seen_count+len(identifiers)
            if count > expected:
                raise ValueError('Incomplete or repeated source cursor')
            done = count == expected
            if not done and (not identifiers or following is None or following == cursor):
                raise ValueError('Incomplete or repeated source cursor')
            values = ['id:'+identifier for identifier in identifiers]
            probes = values+(['cursor:'+json.dumps(following)] if not done else [])
            previous = store.sql('SELECT value FROM wl_harvest_seen '
                'WHERE run_key=? AND scan_key=? AND value IN (SELECT value FROM json_each(?))',
                (run,scan_key,json.dumps(probes))) if probes else []
            if any(row['value'].startswith('id:') for row in previous):
                raise ValueError('Repeated source record during pagination')
            if previous:
                raise ValueError('Incomplete or repeated source cursor')
            ingested = store.sql('SELECT 1 FROM wl_checkpoints WHERE run_key=? AND unit=?',(run,unit+':ingested'))
            if not ingested:
                if ingest_many:
                    ingest_many(page['records'])
                else:
                    for article in page['records']:
                        ingest(article)
            # One small transaction commits seen IDs, ingestion marker and next
            # cursor together. A crash before it replays only this idempotent page.
            # JSON parameters avoid D1's bound-variable limit on 100-record pages.
            progress = dict(cursor=following,expected=expected,seen_count=count,
                            page_number=page_number+1)
            commits = [
                ('INSERT OR IGNORE INTO wl_harvest_seen SELECT ?,?,value FROM json_each(?)',
                 (run,scan_key,json.dumps(values+['cursor:'+json.dumps(cursor)]))),
                ('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',(run,unit+':ingested','true')),
                ('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',
                 (run,progress_unit,json.dumps(progress)))]
            if done:
                commits.append(('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',
                    (run,completed_unit,json.dumps({'count':expected,'generation':generation}))))
            store.batch(commits)
            if done:
                counts[name] = expected
                break
            seen_count = count
            page_number += 1
            cursor = following
    return {'status': 'COMPLETE_FOR_QUERY', 'counts': counts, 'window_start': start, 'window_end': end,
            'checkpoint_namespace':namespace+getattr(adapter, 'checkpoint_namespace', '')}


def harvest_windowed(store, run, adapter, queries, start, end, ingest,
                     ingest_many=None, namespace=''):
    """Freeze small date partitions for fresh work, preserving existing snapshots.

    The original query/window is never changed. Already-started legacy work uses
    its exact cursor instead of discarding costly cached pages to repartition.
    Each fresh <=7-day child is independently checkpointed and fully paginated.
    """
    if not getattr(adapter,'supports_date_slicing',False):
        return harvest(store,run,adapter,queries,start,end,ingest,
                       ingest_many=ingest_many,namespace=namespace)
    from pipeline import digest
    counts, partitions = {}, {}
    for name,query in queries.items():
        identity=digest([name,query,start,end,None])
        prefix=namespace+getattr(adapter,'checkpoint_namespace','')
        complete_unit=prefix+'supplement-complete:'+adapter.name+':'+identity
        complete=store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?',
                           (run,complete_unit))
        if complete:
            receipt=json.loads(complete[0]['payload'])
            counts[name]=receipt['count']
            partitions[name]=receipt.get('partitions',[{'window_start':start,'window_end':end}])
            continue
        def plan():
            legacy=prefix+'supplement:'+adapter.name+':'+digest([name,query,start,end,None])
            if store.sql('SELECT 1 FROM wl_checkpoints WHERE run_key=? AND unit=?',(run,legacy)):
                return {'legacy':True,'windows':[[start,end]]}
            windows=[]
            day,last=dt.date.fromisoformat(start),dt.date.fromisoformat(end)
            if day>last: raise ValueError('Invalid source date window')
            while day<=last:
                stop=min(day+dt.timedelta(days=6),last)
                windows.append([day.isoformat(),stop.isoformat()])
                day=stop+dt.timedelta(days=1)
            return {'legacy':False,'windows':windows}
        frozen=store.checkpoint(run,complete_unit+':window-plan:v1',plan)
        if frozen['legacy']:
            value=harvest(store,run,adapter,{name:query},start,end,ingest,
                ingest_many=ingest_many,namespace=namespace)
            counts[name]=value['counts'][name]
            partitions[name]=[{'window_start':start,'window_end':end}]
            continue
        total, parts=0,[]
        for left,right in frozen['windows']:
            child=harvest(store,run,adapter,{name:query},left,right,ingest,
                ingest_many=ingest_many,namespace=namespace+'window-v1:'+identity+':')
            total+=child['counts'][name]
            parts.append({'window_start':left,'window_end':right,
                          'status':child['status'],'count':child['counts'][name]})
        # These are summed source hits, not an assertion of unique articles across
        # live index snapshots. Canonical ingestion independently deduplicates IDs.
        store.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',
            (run,complete_unit,json.dumps({'count':total,'partitions':parts,
                'count_semantics':'sum_of_disjoint_window_hits'})))
        counts[name],partitions[name]=total,parts
    return {'status':'COMPLETE_FOR_QUERY','counts':counts,'window_start':start,
        'window_end':end,'checkpoint_namespace':namespace+getattr(adapter,'checkpoint_namespace',''),
        'partitions':partitions,'count_semantics':'source_hits_canonical_articles_deduplicated_separately'}




