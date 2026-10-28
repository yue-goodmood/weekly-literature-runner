"""Approved scope-expansion retrieval in original per-source windows.

Separate frozen query/checkpoint namespace; never rewrite original ETL facts,
analyses or dates. READY candidates can be analysed while retrieval continues;
final publication is blocked until the backfill has completed.
"""
import json
from pipeline import (TRIAGE_POLICY, digest, search_split, ingest_articles,
                      select_candidates, score)
from supplements import harvest

PREFIX='backfill:'+TRIAGE_POLICY+':v1:'


def cached(store,run,unit):
    rows=store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?',(run,PREFIX+unit))
    return json.loads(rows[0]['payload']) if rows else None


def save(store,run,unit,value):
    store.sql('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',(run,PREFIX+unit,json.dumps(value)))


def needed(state):
    audit=json.loads(state.get('audit') or '{}')
    scope=audit.get('scope',{})
    return (state.get('status')=='READY' and scope.get('policy')==TRIAGE_POLICY and
            scope.get('coverage')=='legacy_queue_retriage_only')


def plan_for(state,config):
    audit=json.loads(state['audit'])
    plan={'policy':TRIAGE_POLICY,'original_config_hash':state['config_hash'],
          'original_window':[state['window_start'],state['window_end']],
          'sources':{'PubMed':{'window_start':state['window_start'],
             'window_end':state['window_end'],'queries':{
                 'digestive_pelvic':config['queries']['digestive_pelvic']}}}}
    for name,settings in config['supplementary_sources'].items():
        if not settings.get('enabled'):
            plan['sources'][name]={'status':'NOT_ATTEMPTED'};continue
        prior=audit.get('supplementary_sources',{}).get(name,{})
        queries=settings['queries']
        # Digestive-AI query is a strict subset of the broad digestive query.
        # Retrieve once, without losing any AI papers or limiting result counts.
        if 'digestive_pelvic' in queries: queries={'digestive_pelvic':queries['digestive_pelvic']}
        plan['sources'][name]={'window_start':prior.get('window_start',state['window_start']),
            'window_end':prior.get('window_end',state['window_end']),'queries':queries}
    return plan


def resume(store,run,config,pubmed,adapters):
    state=store.sql('SELECT * FROM wl_runs WHERE run_key=?',(run,))[0]
    if not needed(state): return False
    plan=cached(store,run,'plan')
    if plan is None:
        plan=plan_for(state,config)
        save(store,run,'plan',plan)
    if plan['policy']!=TRIAGE_POLICY or plan['original_config_hash']!=state['config_hash']:
        raise ValueError('Backfill frozen identity differs')
    audit=json.loads(state['audit'])
    if audit['scope'].get('backfill',{}).get('status')!='RUNNING':
        audit['scope']['backfill']={'status':'RUNNING','checkpoint_namespace':PREFIX,
            'source_windows':{n:{k:s[k] for k in ('window_start','window_end') if k in s}
                              for n,s in plan['sources'].items()}}
        store.sql('UPDATE wl_runs SET audit=? WHERE run_key=?',(json.dumps(audit),run))
    result={}
    for name,settings in plan['sources'].items():
        if settings.get('status')=='NOT_ATTEMPTED':
            result[name]={'status':'NOT_ATTEMPTED'};continue
        complete=cached(store,run,'source:'+name)
        if complete:
            result[name]=complete;continue
        start,end=settings['window_start'],settings['window_end']
        if name=='PubMed':
            ids=set()
            for label,query in settings['queries'].items():
                for field in ('edat','crdt'):
                    ids.update(search_split(store,pubmed,run,PREFIX+digest(query)+label,
                                            query,start,end,field))
            ids=sorted(ids)
            for offset in range(0,len(ids),80):
                group=ids[offset:offset+80]; unit='pubmed:'+digest(group)
                if cached(store,run,unit): continue
                # Reuse canonical metadata by exact PMID alias; fetch missing only.
                known=store.sql('SELECT l.alias,a.payload FROM wl_aliases l JOIN wl_articles a '
                    'ON a.evidence_key=l.evidence_key WHERE l.alias IN ('+
                    ','.join('?' for _ in group)+')',tuple('PMID:'+x for x in group))
                aliases={r['alias'] for r in known}
                missing=[x for x in group if 'PMID:'+x not in aliases]
                records=store.checkpoint(run,PREFIX+'metadata:'+digest(missing),
                    lambda:pubmed.records(missing)) if missing else []
                ingest_articles(store,run,[json.loads(r['payload']) for r in known]+records)
                save(store,run,unit,{'done':True})
            complete={'status':'COMPLETE_FOR_QUERY','unique_pmids':len(ids),
                      'window_start':start,'window_end':end}
        else:
            if name not in adapters: raise ValueError('Backfill adapter unavailable')
            complete=harvest(store,run,adapters[name],settings['queries'],start,end,
                lambda article:ingest_articles(store,run,[article]),None,
                ingest_many=lambda records:ingest_articles(store,run,records),namespace=PREFIX)
        save(store,run,'source:'+name,complete)
        result[name]=complete
    # Original ETL configuration, windows and source audit remain untouched.
    counts=select_candidates(store,run,None)
    # Reload: concurrent immutable analysis saves must never be overwritten.
    audit=json.loads(store.sql('SELECT audit FROM wl_runs WHERE run_key=?',(run,))[0]['audit'])
    analyzed=store.sql('SELECT count(*) AS n FROM wl_candidates c JOIN wl_analysis a '
        'ON a.run_key=c.run_key AND a.evidence_key=c.evidence_key '
        'WHERE c.run_key=? AND c.selected=1',(run,))[0]['n']
    audit.update(counts,analyzed=analyzed,deferred=0)
    audit['scope'].update(coverage='original_windows_backfilled',expanded_retrieval='COMPLETE_FOR_QUERY',
        backfill={'status':'COMPLETE_FOR_QUERY','checkpoint_namespace':PREFIX,'sources':result})
    store.batch([
        ('UPDATE wl_runs SET audit=? WHERE run_key=?',(json.dumps(audit),run)),
        ('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)',(run,PREFIX+'complete',json.dumps(result)))])
    return True
