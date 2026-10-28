"""Explicit, resumable presentation-only upgrade of code-owned partial pages."""
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from pipeline import request, check_budget, retry_delay
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
    for attempt in range(3):
        check_budget(0.4)
        time.sleep(0.4)  # Stay below Notion's average request allowance.
        req=urllib.request.Request(url,data=json.dumps(body,ensure_ascii=False).encode(),headers=headers(),method='PATCH')
        try:
            with urllib.request.urlopen(req,timeout=60) as response:
                response.read()
            return
        except urllib.error.HTTPError as exc:
            if exc.code!=429 or attempt==2: raise
            time.sleep(retry_delay(exc.headers,attempt))
        # Ambiguous failures never retry an append. Resume via exact readback.


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


def semantic(block):
    """Ignore text segmentation, but retain every character and linked span."""
    value=_canonical(block)
    spans=[]
    for text,link in value.get('content',[]):
        if not text: continue
        if spans and spans[-1][1]==link: spans[-1][0]+=text
        else: spans.append([text,link])
    if 'content' in value: value['content']=spans
    return value


def block_text(block):
    return ''.join(x[0] for x in _canonical(block).get('content',[]))


def validate_title_translation(expected, current):
    """Only title translations are permitted; scientific text/links stay exact."""
    originals={}; translations={}; stripped=[]
    for i,b in enumerate(expected):
        text=block_text(b)
        if b['type']=='heading_2':
            match=re.fullmatch(r'(\d+)\. (.+)',text)
            if not match: raise ValueError('Unrecognized paper heading')
            originals[match[1]]=match[2]
        if b['type']=='bulleted_list_item' and text.startswith('英文原题：'): continue
        stripped.append(b)
    actual=[]
    for i,b in enumerate(current):
        text=block_text(b)
        if b['type']=='heading_2':
            match=re.fullmatch(r'(\d+)\. (.+)',text)
            if not match or match[1] not in originals or not re.search(r'[\u3400-\u9fff]',match[2]):
                raise ValueError('Missing Chinese title binding')
            original=originals[match[1]]
            if i+1>=len(current) or current[i+1]['type']!='bulleted_list_item' or block_text(current[i+1]) not in ('英文原题：'+original,'英文原题： '+original):
                raise ValueError('Original title binding changed')
            if original in translations and translations[original]!=match[2]:
                raise ValueError('Conflicting title translations')
            translations[original]=match[2]
        if b['type']=='bulleted_list_item' and text.startswith('英文原题：'):
            if i==0 or current[i-1]['type']!='heading_2': raise ValueError('Unexpected original title row')
            continue
        actual.append(b)
    if len(stripped)!=len(actual): raise ValueError('Non-title blocks changed')
    for index,(old,new) in enumerate(zip(stripped,actual)):
        wanted=semantic(old); text=block_text(old)
        if old['type']=='heading_2':
            number,original=text.split('. ',1)
            if original not in translations: raise ValueError('Translation missing')
            wanted['content']=[[number+'. '+translations[original],None]]
        elif old['type']=='bulleted_list_item' and '｜' in text:
            prefix,suffix=text.split('｜',1)
            match=re.fullmatch(r'(\[[A-Z]+\] )?(.+)',prefix)
            if match and match[2] in translations:
                if any(link for _,link in wanted.get('content',[])):
                    raise ValueError('Title line contains unexpected link')
                wanted['content']=[[(match[1] or '')+translations[match[2]]+'｜'+suffix,None]]
        received=semantic(new)
        if wanted!=received:
            reason=('type' if wanted.get('type')!=received.get('type') else
                    'text' if ''.join(x[0] for x in wanted.get('content',[]))!=''.join(x[0] for x in received.get('content',[])) else
                    'links' if wanted.get('content')!=received.get('content') else 'icon')
            raise ValueError('Non-title content changed; preserve page: '+str(index)+' '+reason)
    return translations


def adopt_title_translations(store, run, entry):
    """Reconcile authorized display-only edits without rewriting source evidence."""
    from partial_publish import cached, SHOWN
    from report_format import build_report, _block
    artifact='partial:'+run+':'+str(entry['number']); unit='layout:chinese-titles:v1'
    if cached(store,artifact,unit,None): return
    publication=store.sql('SELECT * FROM wl_publications WHERE run_key=?',(artifact,))[0]
    current=read(publication['page_id'])
    actual_hash=fingerprint([_canonical(b) for b in current])
    if publication['status']!='VERIFIED': raise ValueError('Translation requires verified batch')
    if actual_hash==publication['content_hash']:
        checkpoint(store,artifact,unit,{'done':True}); return
    rows=store.sql(
        'SELECT c.evidence_key,n.payload,a.payload AS article FROM wl_candidates c '
        'JOIN wl_analysis n ON n.run_key=c.run_key AND n.evidence_key=c.evidence_key '
        'JOIN wl_articles a ON a.evidence_key=c.evidence_key '
        'JOIN wl_checkpoints p ON p.run_key=c.run_key AND p.unit=?||c.evidence_key '
        'WHERE c.run_key=? AND c.selected=1 AND p.payload=? ORDER BY c.evidence_key',
        (SHOWN,run,artifact))
    if len(rows)!=entry['count']: raise ValueError('Translation batch receipt mismatch')
    state=store.sql('SELECT * FROM wl_runs WHERE run_key=?',(run,))[0]
    blocks,included=build_report(state,rows)
    blocks[0]=_block('callout','处理中｜分批同步，不是最终周报。本批复核'+str(len(rows))+
        '篇、纳入'+str(included)+'篇。摘要分析，待专业审阅。',icon={'type':'emoji','emoji':'📚'})
    blocks[-2]=_block('paragraph','本批送达仅代表上述分析已读回核验，运行仍在处理中；来源覆盖和医学审核均不因此完成。')
    blocks=format_blocks(blocks)
    # The previous renderer lacked original-title rows and used the older label.
    import copy
    legacy=[]
    for block in copy.deepcopy(blocks):
        if block['type']=='bulleted_list_item':
            if block_text(block).startswith('英文原题：'): continue
            for item in block[block['type']]['rich_text']:
                item['text']['content']=item['text']['content'].replace('科普候选／证据边界：','科普候选与证据边界：')
        legacy.append(block)
    expected=next((b for b in (blocks,legacy) if fingerprint([_canonical(x) for x in b])==publication['content_hash']),None)
    if expected is None: raise ValueError('Cannot reconstruct verified pre-translation batch')
    translations=validate_title_translation(expected,current)
    store.batch([
        ('UPDATE wl_publications SET content_hash=? WHERE run_key=?',(actual_hash,artifact)),
        ('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',
         (artifact,unit,json.dumps({'done':True,'translations':translations},ensure_ascii=False)))])


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
