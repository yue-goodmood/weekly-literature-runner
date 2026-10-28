import json
import unittest
from pipeline import Store,TRIAGE_POLICY,ingest_articles,select_candidates
import backfill


class Source:
    def __init__(self): self.searches=0;self.fetched=[];self.fail=False
    def search(self,*args): self.searches+=1;return ['1','2']
    def records(self,ids):
        if self.fail: raise RuntimeError('D1_DAILY_QUOTA_EXCEEDED')
        self.fetched.extend(ids)
        return [{'pmid':x,'title':'Gastric cancer prospective cohort','abstract':'50 patients'} for x in ids]


class Adapter:
    def __init__(self,name,pmid): self.name=name;self.pmid=pmid;self.windows=[]
    def page(self,query,start,end,cursor):
        self.windows.append((start,end))
        return {'records':[{'source':self.name,'source_id':self.pmid,
            'pmid':self.pmid,'title':'Small bowel clinical cohort','abstract':'Patients with bowel disease'}],
            'total':1,'next':None}


class BackfillTest(unittest.TestCase):
    def setUp(self):
        self.store=Store(':memory:');self.store.migrate();self.addCleanup(self.store.db.close)
        self.run='synthetic-run'
        self.audit={'triage':{'policy':TRIAGE_POLICY},'scope':{'policy':TRIAGE_POLICY,
            'coverage':'legacy_queue_retriage_only'},'supplementary_sources':{
                'Europe PMC':{'status':'COMPLETE_FOR_QUERY','window_start':'2026-09-01','window_end':'2026-09-30'},
                'arXiv':{'status':'PARTIAL','window_start':'2026-09-15','window_end':'2026-09-30'}}}
        self.store.sql('INSERT INTO wl_runs VALUES(?,?,?,?,?,?)',
            (self.run,'original-frozen','2026-09-15','2026-09-30','READY',json.dumps(self.audit)))
        ingest_articles(self.store,self.run,[{'pmid':'1','title':'Rectal cancer trial'}])
        select_candidates(self.store,self.run,None)
        self.analysis='{"decision":"include","summary":"immutable"}'
        self.store.sql('INSERT INTO wl_analysis VALUES(?,?,?)',(self.run,'PMID:1',self.analysis))
        self.config={'queries':{'digestive_pelvic':'broad digestive query'},'supplementary_sources':{
            'Europe PMC':{'enabled':True,'queries':{'digestive_pelvic':'broad','digestive_ai':'subset'}},
            'arXiv':{'enabled':True,'queries':{'digestive_ai':'digestive ai'}},
            'Crossref':{'enabled':False}}}
        self.source=Source();self.epmc=Adapter('Europe PMC','3');self.arxiv=Adapter('arXiv','4')

    def run_backfill(self):
        return backfill.resume(self.store,self.run,self.config,self.source,
                               {'Europe PMC':self.epmc,'arXiv':self.arxiv})

    def test_original_windows_metadata_analysis_and_idempotency(self):
        self.assertTrue(self.run_backfill())
        self.assertEqual(self.source.fetched,['2'])
        self.assertEqual(self.epmc.windows,[('2026-09-01','2026-09-30')])
        self.assertEqual(self.arxiv.windows,[('2026-09-15','2026-09-30')])
        state=self.store.sql('SELECT * FROM wl_runs')[0]
        self.assertEqual(state['config_hash'],'original-frozen')
        audit=json.loads(state['audit'])
        self.assertEqual(audit['supplementary_sources'],self.audit['supplementary_sources'])
        self.assertEqual(audit['scope']['backfill']['status'],'COMPLETE_FOR_QUERY')
        self.assertEqual(audit['selected'],4)
        self.assertEqual(self.store.sql('SELECT payload FROM wl_analysis')[0]['payload'],self.analysis)
        self.assertFalse(self.run_backfill())
        self.assertEqual(self.source.searches,2)
        self.assertEqual(self.store.sql('SELECT * FROM wl_history'),[])

    def test_quota_pause_never_marks_complete_and_resumes_frozen_plan(self):
        self.source.fail=True
        with self.assertRaisesRegex(RuntimeError,'D1_DAILY_QUOTA_EXCEEDED'): self.run_backfill()
        self.assertIsNone(backfill.cached(self.store,self.run,'complete'))
        audit=json.loads(self.store.sql('SELECT audit FROM wl_runs')[0]['audit'])
        self.assertEqual(audit['scope']['backfill']['status'],'RUNNING')
        self.source.fail=False
        self.config['queries']['digestive_pelvic']='changed after pause'
        self.run_backfill()
        self.assertEqual(self.source.searches,2)
        plan=backfill.cached(self.store,self.run,'plan')
        self.assertEqual(plan['sources']['PubMed']['queries']['digestive_pelvic'],'broad digestive query')

    def test_query_subset_removed_but_not_ai_evidence(self):
        state=self.store.sql('SELECT * FROM wl_runs')[0]
        plan=backfill.plan_for(state,self.config)
        self.assertEqual(set(plan['sources']['Europe PMC']['queries']),{'digestive_pelvic'})
        self.assertEqual(set(plan['sources']['arXiv']['queries']),{'digestive_ai'})


if __name__=='__main__': unittest.main()
