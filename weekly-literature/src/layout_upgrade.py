"""Explicit, resumable presentation-only upgrade of code-owned partial pages."""
import hashlib
import json
import os
import urllib.request
from pipeline import request
from report_format import _canonical, format_blocks, REPORT_STYLE_VERSION


def fingerprint(blocks):
    return hashlib.sha256(json.dumps({'style':REPORT_STYLE_VERSION,'blocks':blocks},
        ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def headers():
    return {'Authorization':'Bearer '+os.environ['NOTION_TOKEN'],
            'Notion-Version':'2025-09-03','Content-Type':'application/json'}


def read(page):
    blocks=[]; cursor=None
    while True:
        url='https://api.notion.com/v1/blocks/'+page+'/children'+('?start_cursor='+cursor if cursor else '')
        result=json.loads(request(url,headers=headers()))
        blocks.extend(result['results'])
        if not result['has_more']: return blocks
        cursor=result['next_cursor']


def patch(url, body):
    req=urllib.request.Request(url,data=json.dumps(body,ensure_ascii=False).encode(),headers=headers(),method='PATCH')
    with urllib.request.urlopen(req,timeout=60) as response:
        response.read()  # Reconcile writes by readback, never blind append retries.


def native(block):
    kind=block['type']; data=block[kind]
    if kind not in ('paragraph','callout','heading_1','heading_2','bulleted_list_item','table_of_contents') or block.get('has_children'):
        raise ValueError('Unsupported layout block; preserve manually edited page')
    body={k:data[k] for k in ('color','icon') if k in data}
    if kind != 'table_of_contents':
        body['rich_text']=[]
        for item in data.get('rich_text',[]):
            if item.get('type') != 'text':
                raise ValueError('Unexpected native reference; preserve page')
            text={'content':item.get('plain_text',item['text']['content'])}
            link=item.get('href') or (item['text'].get('link') or {}).get('url')
            if link: text['link']={'url':link}
            value={'type':'text','text':text}
            if 'annotations' in item: value['annotations']=item['annotations']
            body['rich_text'].append(value)
    return {'object':'block','type':kind,kind:body}


def checkpoint(store, artifact, unit, value):
    store.sql('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',(artifact,unit,json.dumps(value)))


def restyle_batch(store, artifact):
    from partial_publish import cached
    unit='layout:historical:v2'
    intent=cached(store,artifact,unit,None)
    if intent and intent.get('done'): return
    publication=store.sql('SELECT * FROM wl_publications WHERE run_key=?',(artifact,))[0]
    current=read(publication['page_id'])
    if not intent:
        before=[_canonical(b) for b in current]
        if publication['status']!='VERIFIED' or fingerprint(before)!=publication['content_hash']:
            raise ValueError('Batch edited; preserve content rather than restyle')
        desired=format_blocks([native(b) for b in current])
        intent={'ids':[b['id'] for b in current],'before':before,'desired':desired}
        checkpoint(store,artifact,unit,intent)
    desired=intent['desired']; after=[_canonical(b) for b in desired]
    if [b['id'] for b in current]!=intent['ids'] or any(
            _canonical(b) not in (intent['before'][i],after[i]) for i,b in enumerate(current)):
        raise ValueError('Batch changed during style update; preserve edits')
    for old,new in zip(current,desired):
        kind=new['type']
        if native(old)[kind]!=new[kind]:
            patch('https://api.notion.com/v1/blocks/'+old['id'],{kind:new[kind]})
    if [_canonical(b) for b in read(publication['page_id'])]!=after:
        raise ValueError('Styled batch readback mismatch')
    store.sql('UPDATE wl_publications SET content_hash=? WHERE run_key=?',(fingerprint(after),artifact))
    checkpoint(store,artifact,unit,{'done':True})


def upgrade_progress(store, run, entries):
    from partial_publish import cached, legacy_progress_blocks, progress_blocks
    artifact='progress:'+run; unit='layout:progress:v2'
    intent=cached(store,artifact,unit,None)
    if intent and intent.get('done'): return
    publication=store.sql('SELECT * FROM wl_publications WHERE run_key=?',(artifact,))[0]
    current=read(publication['page_id'])
    desired=progress_blocks(run,entries); after=[_canonical(b) for b in desired]
    if not intent:
        before=[_canonical(b) for b in legacy_progress_blocks(run,entries)]
        actual=[_canonical(b) for b in current]
        if actual==after and publication['content_hash']==fingerprint(after):
            checkpoint(store,artifact,unit,{'done':True}); return
        if actual!=before or publication['content_hash']!=fingerprint(before) or any(b.get('has_children') for b in current):
            raise ValueError('Progress page differs; preserve manual edits')
        # Freeze IDs before archiving only the exactly verified generated index.
        intent={'old':{b['id']:_canonical(b) for b in current},'desired':desired}
        checkpoint(store,artifact,unit,intent)
    desired=intent['desired']; after=[_canonical(b) for b in desired]
    old=[]; appended=[]; started=False
    for b in current:
        if b['id'] in intent['old'] and not started:
            if _canonical(b)!=intent['old'][b['id']]: raise ValueError('Index edited during migration')
            old.append(b)
        else:
            started=True; appended.append(_canonical(b))
    if appended!=after[:len(appended)]: raise ValueError('Unexpected index content; preserve edits')
    for b in old:
        patch('https://api.notion.com/v1/blocks/'+b['id'],{'archived':True})
    for offset in range(len(appended),len(desired),50):
        patch('https://api.notion.com/v1/blocks/'+publication['page_id']+'/children',{'children':desired[offset:offset+50]})
    if [_canonical(b) for b in read(publication['page_id'])]!=after:
        raise ValueError('Index layout readback mismatch')
    store.sql('UPDATE wl_publications SET content_hash=? WHERE run_key=?',(fingerprint(after),artifact))
    checkpoint(store,artifact,unit,{'done':True})
