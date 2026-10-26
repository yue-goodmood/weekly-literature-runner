"""Public-source adapters. Every result page is independently resumable."""
import html
import itertools
import json
import re
import time
import urllib.parse
import xml.etree.ElementTree as ET


def plain(value):
    return html.unescape(re.sub(r'<[^>]+>', ' ', value or '')).strip()


class EuropePMC:
    name = 'Europe PMC'
    checkpoint_namespace = 'epmc-date-v2:'

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
        cursor, seen, used = None, set(), set()
        expected = None
        for page_number in itertools.count():
            if max_pages is not None and page_number >= max_pages:
                raise ValueError('Source page budget reached; coverage remains partial')
            from pipeline import digest
            unit = namespace+getattr(adapter, 'checkpoint_namespace', '')+'supplement:'+adapter.name+':'+digest([name, query, start, end, cursor])
            page = store.checkpoint(run, unit, lambda: adapter.page(query, start, end, cursor))
            if expected is None:
                expected = page['total']
            if page['total'] != expected:
                raise ValueError('Source count changed during pagination; coverage remains partial')
            ingested = store.sql('SELECT 1 FROM wl_checkpoints WHERE run_key=? AND unit=?',(run,unit+':ingested'))
            for article in page['records']:
                identifier = article['source_id']
                if identifier in seen:
                    raise ValueError('Repeated source record during pagination: '+json.dumps(
                        dict(query=name,page=page_number+1,source_id=identifier,
                             unique_seen=len(seen),total=expected)))
                seen.add(identifier)
            if not ingested:
                if ingest_many:
                    ingest_many(page['records'])
                else:
                    for article in page['records']:
                        ingest(article)
            store.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',(run,unit+':ingested','true'))
            if len(seen) == expected:
                counts[name] = expected
                break
            following = page['next']
            if len(seen) > expected or not page['records'] or following is None or following in used or following == cursor:
                raise ValueError('Incomplete or repeated source cursor')
            used.add(following)
            cursor = following
    return {'status': 'COMPLETE_FOR_QUERY', 'counts': counts, 'window_start': start, 'window_end': end,
            'checkpoint_namespace':namespace+getattr(adapter, 'checkpoint_namespace', '')}



