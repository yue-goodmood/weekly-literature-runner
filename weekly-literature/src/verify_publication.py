"""Publish a fully analysed run, replay it, and independently verify Notion/D1."""
import hashlib
import json
import os
from pipeline import Store, request
from publish import publish
from report_format import _canonical, REPORT_STYLE_VERSION

def verify(store, run):
    headers={'Authorization':'Bearer '+os.environ['NOTION_TOKEN'],'Notion-Version':'2025-09-03','Content-Type':'application/json'}
    root='https://api.notion.com/v1'
    from partial_publish import final_artifact
    state=store.sql('SELECT * FROM wl_runs WHERE run_key=?',(run,))[0]
    publications=store.sql('SELECT * FROM wl_publications WHERE run_key=? OR run_key IN '
        '(SELECT \'popscience:\'||a.evidence_key FROM wl_analysis a JOIN wl_candidates c '
        'ON c.run_key=a.run_key AND c.evidence_key=a.evidence_key '
        'WHERE a.run_key=? AND c.selected=1 AND json_extract(a.payload,\'$.decision\')=\'include\' '
        'AND json_extract(a.payload,\'$.popscience_priority\')=\'high\')',(final_artifact(state),run))
    result=[]
    for row in publications:
        page=json.loads(request(root+'/pages/'+row['page_id'],headers=headers))
        if page.get('archived') or page.get('in_trash'):
            raise ValueError('Publication page archived')
        texts=[]; fetched=[]; cursor=None
        while True:
            url=root+'/blocks/'+row['page_id']+'/children'+('?start_cursor='+cursor if cursor else '')
            data=json.loads(request(url,headers=headers))
            fetched.extend(data['results'])
            if not data['has_more']:break
            cursor=data['next_cursor']
        if row['run_key'].startswith('weekly-literature:') and fetched and fetched[0]['type']=='callout':
            normalized=[_canonical(block) for block in fetched]
            actual=hashlib.sha256(json.dumps(
                {'style':REPORT_STYLE_VERSION,'blocks':normalized},ensure_ascii=False,
                sort_keys=True,separators=(',',':')).encode()).hexdigest()
        else:
            # Keep supporting publication hashes created by the legacy
            # paragraph-only writer, including existing high-priority articles.
            for block in fetched:
                if block['type']!='paragraph':raise ValueError('Unexpected legacy block type')
                texts.append(''.join(t['plain_text'] for t in block['paragraph']['rich_text']))
            actual=hashlib.sha256(''.join(texts).encode()).hexdigest()
        if row['status']!='VERIFIED' or actual!=row['content_hash']:
            raise ValueError('Independent Notion readback mismatch')
        title_prop=next(k for k,v in page['properties'].items() if v['type']=='title')
        rendered_title=''.join(
            t.get('plain_text',t.get('text',{}).get('content',''))
            for t in page['properties'][title_prop]['title'])
        query={'filter':{'property':title_prop,'title':{'equals':rendered_title}},'page_size':2}
        matches=json.loads(request(root+'/data_sources/'+page['parent']['data_source_id']+'/query',json.dumps(query).encode(),headers))
        if len(matches['results'])!=1 or matches.get('has_more') or matches['results'][0]['id']!=page['id']:
            raise ValueError('Duplicate or missing publication page')
        if row['run_key'].startswith('popscience:'):
            for key,expected in {'医学审核':'未审核','状态':'待医学审核','润色状态':'未润色'}.items():
                prop=page['properties'][key]
                if prop[prop['type']]['name']!=expected:raise ValueError('Review status mismatch')
        result.append({'artifact':row['run_key'],'page_id':page['id'],'url':page['url'],'content_hash':actual,'blocks':len(fetched)})
    return sorted(result,key=lambda x:x['artifact'])

def main():
    run=os.environ['RUN_KEY'];store=Store()
    rows=store.sql('SELECT c.evidence_key,n.payload FROM wl_candidates c LEFT JOIN wl_analysis n ON n.run_key=c.run_key AND n.evidence_key=c.evidence_key WHERE c.run_key=? AND c.selected=1',(run,))
    if not rows or any(r['payload'] is None for r in rows):raise ValueError('Incomplete analysis')
    included={r['evidence_key'] for r in rows if json.loads(r['payload'])['decision']=='include'}
    high=sum(json.loads(r['payload']).get('popscience_priority')=='high' for r in rows)
    publish(store,run);first=verify(store,run)
    publish(store,run);second=verify(store,run)
    if first!=second or len(first)!=1+high:raise ValueError('Publication replay mismatch')
    history={r['evidence_key'] for r in store.sql('SELECT evidence_key FROM wl_history WHERE run_key=?',(run,))}
    if history!=included:raise ValueError('Published history differs from included decisions')
    state=store.sql('SELECT status,audit FROM wl_runs WHERE run_key=?',(run,))[0]
    if state['status']!='PUBLISHED':raise ValueError('Run not published')
    print(json.dumps({'run_key':run,'status':state['status'],'analysed':len(rows),'included':len(included),'excluded':len(rows)-len(included),'history_verified':len(history),'repeat_verified':True,'artifacts':first,'audit':json.loads(state['audit'])},ensure_ascii=False))

if __name__=='__main__':main()



