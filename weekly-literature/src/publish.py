"""Publish to a stable Notion page with durable creation intent and readback."""
import argparse
import hashlib
import json
import os
import re
from pipeline import Store, request
from report_format import build_report, publish_report_blocks, update_report_metadata, _block

def finalize_article_title(page, artifact, title):
    """Replace only our creation placeholder; never overwrite a user's title."""
    headers = {'Authorization':'Bearer '+os.environ['NOTION_TOKEN'], 'Notion-Version':'2025-09-03', 'Content-Type':'application/json'}
    url = 'https://api.notion.com/v1/pages/'+page
    def read_title():
        data = json.loads(request(url, headers=headers))
        name = next(k for k,v in data['properties'].items() if v['type']=='title')
        text = ''.join(t.get('plain_text',t.get('text',{}).get('content','')) for t in data['properties'][name]['title'])
        return name, text
    name, existing = read_title()
    if existing == title:
        return
    if existing != artifact:
        raise ValueError('Article title edited; preserve user changes')
    import urllib.request
    payload = {'properties':{name:{'title':[{'text':{'content':title}}]}}}
    req = urllib.request.Request(url,data=json.dumps(payload).encode(),headers=headers,method='PATCH')
    with urllib.request.urlopen(req,timeout=60) as response:
        response.read()
    if read_title()[1] != title:
        raise ValueError('Article title readback mismatch')


def verify_artifacts(store, run, expected):
    from verify_publication import verify
    verified = verify(store, run)
    if {row['artifact'] for row in verified} != expected:
        raise ValueError('Required report/article publication missing')

def publish_draft(store, row):
    """Deliver the same immutable medical-review draft in partial and final runs."""
    value = json.loads(row['payload'])
    if value.get('popscience_priority') != 'high':
        return
    draft = value.get('popscience_article')
    if value['decision'] != 'include' or not draft or not draft.get('body') or not draft.get('references'):
        raise ValueError('High priority article missing; do not mark run complete')
    artifact = 'popscience:' + row['evidence_key']
    target = provision_page(store, artifact, os.environ['NOTION_ARTICLE_DATA_SOURCE_ID'], review=True)
    text = draft['title']+'\n\n'+draft['body']+'\n\n参考来源\n'+'\n'.join(draft['references'])+'\n\n证据键：'+row['evidence_key']+'\n医学审核：未审核；状态：待医学审核；润色状态：未润色'
    publish_text(store, artifact, target, text)
    finalize_article_title(target, artifact, draft['title'])

def provision_page(store, run, data_source=None, review=False):
    """Reconcile a create intent before any retry of Notion's non-idempotent POST."""
    existing = store.sql('SELECT page_id FROM wl_publications WHERE run_key=?', (run,))
    if existing:
        return existing[0]['page_id']
    database = data_source or os.environ['NOTION_DATA_SOURCE_ID']
    headers = {'Authorization':'Bearer '+os.environ['NOTION_TOKEN'], 'Notion-Version':'2025-09-03', 'Content-Type':'application/json'}
    root = 'https://api.notion.com/v1'
    schema = json.loads(request(root+'/data_sources/'+database, headers=headers))
    title_prop = next(name for name,value in schema['properties'].items() if value['type']=='title')
    query = {'filter':{'property':title_prop,'title':{'equals':run}}, 'page_size':2}
    found = json.loads(request(root+'/data_sources/'+database+'/query',json.dumps(query).encode(),headers))
    if len(found['results']) > 1 or found.get('has_more'):
        raise ValueError('Multiple Notion pages for run key')
    if found['results']:
        page = found['results'][0]['id']
    else:
        if store.sql('SELECT 1 FROM wl_page_intents WHERE run_key=?',(run,)):
            raise ValueError('Previous page create outcome unknown; reconcile Notion before retrying creation')
        body={'parent':{'type':'data_source_id','data_source_id':database},'properties':{title_prop:{'title':[{'text':{'content':run}}]}}}
        if not review:
            body['icon']={'type':'emoji','emoji':'📫'}
            day=next((p for p in run.split(':') if re.fullmatch(r'\d{4}-\d{2}-\d{2}',p)),None)
            if day and schema['properties'].get('执行日期',{}).get('type')=='date':
                body['properties']['执行日期']={'date':{'start':day}}
        if review:
            for name,option in {'医学审核':'未审核','状态':'待医学审核','润色状态':'未润色'}.items():
                definition=schema['properties'].get(name,{})
                kind=definition.get('type')
                if kind not in ('select','status') or option not in [x['name'] for x in definition[kind]['options']]:
                    raise ValueError('Missing existing Notion review option: '+name)
                body['properties'][name]={kind:{'name':option}}
        import urllib.request
        store.sql('INSERT INTO wl_page_intents VALUES(?)',(run,))
        req=urllib.request.Request(root+'/pages',data=json.dumps(body).encode(),headers=headers,method='POST')
        # No retry: an ambiguous response must be reconciled by exact title query.
        with urllib.request.urlopen(req,timeout=60) as response:
            page=json.loads(response.read())['id']
    store.sql('INSERT OR IGNORE INTO wl_publications(run_key,page_id) VALUES(?,?)',(run,page))
    return page

def publish(store, run):
    state = store.sql('SELECT * FROM wl_runs WHERE run_key=?', (run,))[0]
    if state['status'] == 'PUBLISHED':
        return
    if state['status'] not in ('READY', 'PUBLISHING'):
        raise ValueError('ETL incomplete')
    rows = store.sql('SELECT c.evidence_key,n.payload,a.payload AS article FROM wl_candidates c JOIN wl_articles a USING(evidence_key) LEFT JOIN wl_analysis n ON n.run_key=c.run_key AND n.evidence_key=c.evidence_key WHERE c.run_key=? AND c.selected=1 ORDER BY c.score DESC,c.evidence_key', (run,))
    selected = store.sql('SELECT count(*) n FROM wl_candidates WHERE run_key=? AND selected=1', (run,))[0]['n']
    if len(rows) != selected:
        raise ValueError('Candidate metadata missing; refuse truncated publication')
    if any(row['payload'] is None for row in rows):
        raise ValueError('Missing AI decisions; resume only outstanding candidates')
    audit = json.loads(state['audit'])
    from partial_publish import cached, INDEX, has_unsynced, final_index
    partial_entries = cached(store, run, INDEX, [])
    if partial_entries and has_unsynced(store, run):
        raise ValueError('Partial Notion delivery outstanding; resume batches before finalization')
    if audit.get('review_policy') == 'unlimited-v1' and store.sql(
            'SELECT 1 FROM wl_candidates c WHERE c.run_key=? AND c.selected=0 '
            'AND NOT EXISTS (SELECT 1 FROM wl_history h WHERE h.evidence_key=c.evidence_key) '
            'AND NOT EXISTS (SELECT 1 FROM wl_candidates old WHERE old.evidence_key=c.evidence_key '
            'AND old.run_key<c.run_key AND old.selected=1) LIMIT 1', (run,)):
        raise ValueError('Unqueued eligible candidates; refuse final publication')
    store.sql("UPDATE wl_runs SET status='PUBLISHING' WHERE run_key=? AND status='READY'", (run,))
    page = provision_page(store, run)
    recovery = store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?',
                         (run, 'recovery:unlimited-v1:publication-base'))
    if recovery:
        base = json.loads(recovery[0]['payload'])
        prior = set(base['evidence_keys'])
        additional = [row for row in rows if row['evidence_key'] not in prior]
        blocks, _ = (final_index(state, rows, partial_entries) if partial_entries else build_report(state, additional))
        blocks.insert(0, _block('heading_1', '回溯补充：解除100条限制后的完整候选复核'))
        blocks.insert(1, _block('paragraph', '保留上方原始归档与审计；以下补入先前被顺延的文献。最新总数与覆盖状态以数据库属性及本次审计为准。'))
        included = sum(json.loads(row['payload'])['decision']=='include' for row in rows)
        publish_report_blocks(store, run, page, blocks, os.environ['NOTION_TOKEN'], prefix=base['blocks'])
    else:
        blocks, included = (final_index(state, rows, partial_entries) if partial_entries else build_report(state, rows))
        publish_report_blocks(store, run, page, blocks, os.environ['NOTION_TOKEN'])
    failures=[]
    for row in rows:
        value=json.loads(row['payload'])
        if value.get('popscience_priority')!='high':
            continue
        draft=value.get('popscience_article')
        if value['decision']!='include' or not draft or not draft.get('body') or not draft.get('references'):
            raise ValueError('High priority article missing; do not mark run complete')
        try:
            publish_draft(store, row)
        except (ValueError, OSError, KeyError) as exc:
            failures.append(row['evidence_key']+':'+type(exc).__name__)
    if failures:
        raise ValueError('Article publication incomplete: '+', '.join(failures))
    included_keys = {row['evidence_key'] for row in rows if json.loads(row['payload'])['decision']=='include'}
    expected = {run} | {'popscience:'+row['evidence_key'] for row in rows
                        if json.loads(row['payload']).get('popscience_priority')=='high'}
    verify_artifacts(store,run,expected)
    store.batch([('INSERT OR IGNORE INTO wl_history VALUES(?,?)', (key,run)) for key in sorted(included_keys)])
    history = {row['evidence_key'] for row in store.sql('SELECT evidence_key FROM wl_history WHERE run_key=?', (run,))}
    if history != included_keys:
        raise ValueError('Published history differs from included decisions')
    # Only after independent body/hash/uniqueness verification may the card
    # receive its final title/date/count/status. Properties are read back too.
    update_report_metadata(run, page, included, audit,
                           os.environ['NOTION_TOKEN'], os.environ['NOTION_DATA_SOURCE_ID'])
    store.sql("UPDATE wl_runs SET status='PUBLISHED' WHERE run_key=?", (run,))

def publish_text(store, artifact, page, text):
    content_hash = hashlib.sha256(text.encode()).hexdigest()
    saved=store.sql('SELECT status,content_hash FROM wl_publications WHERE run_key=?',(artifact,))[0]
    if saved['status']=='VERIFIED':
        if saved['content_hash']!=content_hash:
            raise ValueError('Verified artifact content conflict')
        return
    # Deterministic 1500-character blocks. Read before append; ambiguous writes are
    # reconciled on the next run, never retried blindly.
    chunks = [text[i:i+1500] for i in range(0,len(text),1500)]
    headers = {'Authorization':'Bearer '+os.environ['NOTION_TOKEN'], 'Notion-Version':'2025-09-03','Content-Type':'application/json'}
    base = 'https://api.notion.com/v1/blocks/' + page + '/children'
    def read():
        items=[]; cursor=None
        while True:
            url=base + ('?start_cursor='+cursor if cursor else '')
            value=json.loads(request(url,headers=headers)); items.extend(value['results'])
            if not value['has_more']: break
            cursor=value['next_cursor']
        return [''.join(t.get('plain_text',t.get('text',{}).get('content','')) for t in b.get('paragraph',{}).get('rich_text',[])) for b in items]
    existing=read()
    if existing != chunks[:len(existing)] or len(existing)>len(chunks):
        raise ValueError('Output page differs; preserve user edits and reconcile manually')
    import urllib.request
    for chunk in chunks[len(existing):]:
        payload={'children':[{'object':'block','type':'paragraph','paragraph':{'rich_text':[{'type':'text','text':{'content':chunk}}]}}]}
        req=urllib.request.Request(base, data=json.dumps(payload).encode(),headers=headers,method='PATCH')
        with urllib.request.urlopen(req,timeout=60) as response:
            response.read() # No automatic retry for non-idempotent append.
    if read()!=chunks:
        raise ValueError('Notion readback mismatch')
    store.sql("UPDATE wl_publications SET status='VERIFIED',content_hash=? WHERE run_key=?", (content_hash,artifact))

if __name__=='__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('run_key'); args=parser.parse_args()
    publish(Store(),args.run_key)




