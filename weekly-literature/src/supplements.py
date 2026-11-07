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
    # A different page size must not reuse old cursor/page checkpoints.
    checkpoint_namespace = 'epmc-date-v3-1000:'
    supports_date_slicing = True

    def __init__(self, transport):
        self.transport = transport

    def page(self, query, start, end, cursor):
        params = {'query': f'({query}) AND FIRST_IDATE:[{start} TO {end}]',
                  'format': 'json', 'resultType': 'core', 'pageSize': 1000,
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


def identity_ledger(store, run, scan_key):
    """Exact identities in compact page receipts, not one indexed row per ID.

    Load only IDs/cursors once per resumed query, never saved abstracts. Legacy
    indexed rows migrate once in bounded chunks; originals are never deleted.
    A ready marker is committed last, making partial migrations replay-safe.
    """
    prefix='harvest-identities-v2:'+scan_key+':'
    ready=store.sql('SELECT 1 FROM wl_checkpoints WHERE run_key=? AND unit=?',(run,prefix+'ready'))
    if not ready:
        values=[row['value'] for row in store.sql(
            'SELECT value FROM wl_harvest_seen WHERE run_key=? AND scan_key=? ORDER BY value',(run,scan_key))]
        writes=[('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',
                 (run,prefix+'values:legacy:'+str(offset),json.dumps(values[offset:offset+1000])))
                for offset in range(0,len(values),1000)]
        # Store.batch commits at most 50 statements at once. Keep the marker
        # LAST, so a crash can never make a partial migration authoritative.
        writes.append(('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',(run,prefix+'ready','true')))
        store.batch(writes)
        return prefix,set(values)
    rows=store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit>=? AND unit<?',
                   (run,prefix+'values:',prefix+'values;'))
    return prefix,{value for row in rows for value in json.loads(row['payload'])}


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
        identity_prefix,seen=identity_ledger(store,run,scan_key)
        if sum(value.startswith('id:') for value in seen)!=seen_count:
            raise ValueError('Incomplete or repeated source cursor')
        def retire(reason, observed=None):
            # Preserve the bad page and ledger for diagnosis, but never replay
            # that same poisoned cache on every future scheduled invocation.
            receipt = {'generation':generation+1, 'reason':reason,
                       'previous_total':expected, 'observed_total':observed}
            store.sql('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',
                      (run,snapshot_unit,json.dumps(receipt)))
            failure=SourceSnapshotChanged()
            failure.reason=reason
            failure.partition_window=[start,end]
            failure.previous_total=expected
            failure.observed_total=observed
            ids=[article['source_id'] for article in page['records']]
            failure.source_ids=sorted({value[3:] for value in seen if value.startswith('id:')}|set(ids))
            failure.available_records=page['records']
            failure.generation=generation
            failure.terminal_gap=(reason=='SOURCE_CURSOR_INVALID' and page['next'] is None
                and len(ids)==len(set(ids)) and not seen.intersection('id:'+i for i in ids)
                and len(failure.source_ids)<expected)
            raise failure
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
                retire('SOURCE_COUNT_CHANGED',page['total'])
            identifiers = [article['source_id'] for article in page['records']]
            if len(identifiers) != len(set(identifiers)):
                if getattr(adapter,'supports_date_slicing',False):
                    retire('SOURCE_RECORD_REPEATED_WITHIN_PAGE',page['total'])
                raise ValueError('Repeated source record within page')
            following = page['next']
            count = seen_count+len(identifiers)
            if count > expected:
                if getattr(adapter,'supports_date_slicing',False):
                    retire('SOURCE_COUNT_OVERFLOW',page['total'])
                raise ValueError('Incomplete or repeated source cursor')
            done = count == expected
            if not done and (not identifiers or following is None or following == cursor):
                if getattr(adapter,'supports_date_slicing',False):
                    retire('SOURCE_CURSOR_INVALID',page['total'])
                raise ValueError('Incomplete or repeated source cursor')
            values = ['id:'+identifier for identifier in identifiers]
            probes = values+(['cursor:'+json.dumps(following)] if not done else [])
            previous = seen.intersection(probes)
            if any(value.startswith('id:') for value in previous):
                if getattr(adapter,'supports_date_slicing',False):
                    retire('SOURCE_RECORD_REPEATED',page['total'])
                raise ValueError('Repeated source record during pagination')
            if previous:
                if getattr(adapter,'supports_date_slicing',False):
                    retire('SOURCE_CURSOR_REPEATED',page['total'])
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
                ('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',
                 (run,identity_prefix+'values:page:'+str(page_number),
                  json.dumps(values+['cursor:'+json.dumps(cursor)]))),
                ('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',(run,unit+':ingested','true')),
                ('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',
                 (run,progress_unit,json.dumps(progress)))]
            if done:
                commits.append(('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',
                    (run,completed_unit,json.dumps({'count':expected,'generation':generation}))))
            store.batch(commits)
            seen.update(values+['cursor:'+json.dumps(cursor)])
            if done:
                counts[name] = expected
                break
            seen_count = count
            page_number += 1
            cursor = following
    return {'status': 'COMPLETE_FOR_QUERY', 'counts': counts, 'window_start': start, 'window_end': end,
            'checkpoint_namespace':namespace+getattr(adapter, 'checkpoint_namespace', '')}


def harvest_partition(store, run, adapter, name, query, start, end, ingest,
                      ingest_many, namespace, _confirmation=False):
    """Replace only invalid spans with durable, disjoint smaller date spans.

    A split is committed before child work. On resume a poisoned parent is
    bypassed; completed siblings remain cached. A one-day anomaly pauses after
    retiring its generation, and can never be labelled complete by dedup alone.
    """
    from pipeline import digest
    unit=namespace+getattr(adapter,'checkpoint_namespace','')+'partition-split:v1:'+digest([name,query,start,end])
    resolved=store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?',
                       (run,unit+':resolved-gap:v1'))
    if resolved:
        return [json.loads(resolved[0]['payload'])]
    rows=store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?',(run,unit))
    split=json.loads(rows[0]['payload']) if rows else None
    if not split:
        try:
            result=harvest(store,run,adapter,{name:query},start,end,ingest,
                           ingest_many=ingest_many,namespace=namespace)
            return [{'window_start':start,'window_end':end,
                     'status':result['status'],'count':result['counts'][name]}]
        except SourceSnapshotChanged as exc:
            left,right=dt.date.fromisoformat(start),dt.date.fromisoformat(end)
            if left==right:
                # A terminal upstream response can report deleted/unavailable
                # records in its total while exposing no further cursor. Two
                # fresh generations must agree before resolving WITH A GAP.
                # This never creates a COMPLETE_FOR_QUERY receipt.
                if getattr(exc,'terminal_gap',False):
                    if ingest_many:
                        ingest_many(exc.available_records)
                    else:
                        for article in exc.available_records: ingest(article)
                    key=unit+':gap-observation:v1'
                    prior=store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?',(run,key))
                    prior=json.loads(prior[0]['payload']) if prior else {}
                    fingerprint=digest([exc.previous_total,exc.source_ids])
                    attempts=(prior.get('observations',0) if prior.get('fingerprint')==fingerprint else 0)
                    attempts+=int(prior.get('generation')!=exc.generation)
                    observation={'fingerprint':fingerprint,'generation':exc.generation,'observations':attempts}
                    store.sql('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',(run,key,json.dumps(observation)))
                    if attempts>=2:
                        part={'window_start':start,'window_end':end,'status':'PARTIAL',
                            'reason':'UPSTREAM_TERMINAL_COUNT_MISMATCH',
                            'count':len(exc.source_ids),'reported_total':exc.previous_total,
                            'missing_count':exc.previous_total-len(exc.source_ids),
                            'independent_observations':attempts}
                        store.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',
                                  (run,unit+':resolved-gap:v1',json.dumps(part)))
                        return [part]
                    if not _confirmation:
                        # At most one fresh confirmation in this invocation.
                        # Drift on that confirmation defers to the next run.
                        return harvest_partition(store,run,adapter,name,query,start,end,
                                                 ingest,ingest_many,namespace,True)
                raise
            middle=left+(right-left)//2
            split={'windows':[[start,middle.isoformat()],
                              [(middle+dt.timedelta(days=1)).isoformat(),end]]}
            store.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',
                      (run,unit,json.dumps(split)))
    parts=[]
    for left,right in split['windows']:
        parts.extend(harvest_partition(store,run,adapter,name,query,left,right,
                                      ingest,ingest_many,namespace))
    return parts


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
    gaps=[]
    for name,query in queries.items():
        identity=digest([name,query,start,end,None])
        prefix=namespace+getattr(adapter,'checkpoint_namespace','')
        complete_unit=prefix+'supplement-complete:'+adapter.name+':'+identity
        complete=store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit IN (?,?)',
                           (run,complete_unit,complete_unit+':resolved-partial:v1'))
        if complete:
            receipt=json.loads(complete[0]['payload'])
            counts[name]=receipt['count']
            partitions[name]=receipt.get('partitions',[{'window_start':start,'window_end':end}])
            gaps.extend(p for p in partitions[name] if p.get('status')=='PARTIAL')
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
            windows=[[start,end]]
            child_namespace=namespace
        else:
            windows=frozen['windows']
            child_namespace=namespace+'window-v1:'+identity+':'
        total, parts=0,[]
        for left,right in windows:
            children=harvest_partition(store,run,adapter,name,query,left,right,
                                      ingest,ingest_many,child_namespace)
            total+=sum(child['count'] for child in children)
            parts.extend(children)
        # These are summed source hits, not an assertion of unique articles across
        # live index snapshots. Canonical ingestion independently deduplicates IDs.
        partial=any(part['status']=='PARTIAL' for part in parts)
        gaps.extend(part for part in parts if part['status']=='PARTIAL')
        store.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',
            (run,complete_unit+(':resolved-partial:v1' if partial else ''),json.dumps({'count':total,'partitions':parts,
                'count_semantics':'sum_of_disjoint_window_hits'})))
        counts[name],partitions[name]=total,parts
    return {'status':'PARTIAL' if gaps else 'COMPLETE_FOR_QUERY','counts':counts,'window_start':start,
        'window_end':end,'checkpoint_namespace':namespace+getattr(adapter,'checkpoint_namespace',''),
        'partitions':partitions,'gaps':gaps,'missing_count':sum(g['missing_count'] for g in gaps),
        'count_semantics':'retrieved_source_hits_canonical_articles_deduplicated_separately'}





