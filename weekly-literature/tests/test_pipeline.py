import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('pipeline', ROOT/'src/pipeline.py')
p=importlib.util.module_from_spec(spec); spec.loader.exec_module(p)

class Source:
    def __init__(self): self.calls=0; self.fail=False
    def search(self,*args): self.calls+=1; return ['1','2','3']
    def records(self,ids):
        self.calls+=1
        if self.fail: raise RuntimeError('upstream failure')
        return [dict(pmid=i,title=t,abstract='randomized clinical patient study',doi=d,types=[]) for i,t,d in [
            ('1','Rectal cancer organ preservation','10.1/test'),('2','Rectal cancer duplicate','https://doi.org/10.1/TEST.'),('3','Diabetes medication','10.1/other')]]

class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.store=p.Store(':memory:'); self.store.migrate(); self.addCleanup(self.store.db.close)
        self.config=json.loads((ROOT/'config/topics.json').read_text()); self.source=Source()
    def test_resume_and_deduplicate(self):
        self.source.fail=True
        with self.assertRaises(RuntimeError): p.run_etl(self.store,self.source,self.config,'2026-09-22')
        calls=self.source.calls
        self.source.fail=False
        run=p.run_etl(self.store,self.source,self.config,'2026-09-22')
        self.assertEqual(self.source.calls,calls+1)
        self.assertEqual(len(self.store.sql('SELECT * FROM wl_candidates')),1)
        p.run_etl(self.store,self.source,self.config,'2026-09-22')
        self.assertEqual(self.source.calls,calls+1)
        self.assertEqual(self.store.sql('SELECT status FROM wl_runs')[0]['status'],'READY')
    def test_old_ready_does_not_block_new_week_or_duplicate_selections(self):
        self.config['max_candidates']=10
        old=p.run_etl(self.store,self.source,self.config,'2026-10-06')
        self.assertEqual(self.store.sql('SELECT status FROM wl_runs WHERE run_key=?',(old,))[0]['status'],'READY')
        new=p.run_etl(self.store,self.source,self.config,'2026-10-12')
        self.assertNotEqual(old,new)
        self.assertEqual(self.store.sql('SELECT count(*) n FROM wl_candidates WHERE run_key=? AND selected=1',(old,))[0]['n'],1)
        self.assertEqual(self.store.sql('SELECT count(*) n FROM wl_candidates WHERE run_key=? AND selected=1',(new,))[0]['n'],0)
        self.assertEqual(self.store.sql('SELECT count(*) n FROM wl_runs')[0]['n'],2)

    def test_pubmed_error_payload_is_retried(self):
        source=p.PubMed()
        with patch.object(source,'call',side_effect=[b'{"esearchresult":{"ERROR":"temporary"}}',b'{"esearchresult":{"count":"1","idlist":["7"]}}']), patch.object(p.time,'sleep'):
            self.assertEqual(source.search('query','2026-09-01','2026-09-02','edat'),['7'])

    def test_medical_ai_not_crowded_out(self):
        self.config['max_candidates']=10
        class Mixed(Source):
            def search(self,*args): return [str(i) for i in range(20)]
            def records(self,ids): return [dict(pmid=i,title=('Rectal cancer study ' if int(i)<10 else 'Machine learning for medical diagnosis ')+i,abstract='retrospective cohort of 50 patients',types=[]) for i in ids]
        p.run_etl(self.store,Mixed(),self.config,'2026-09-22')
        self.assertEqual(self.store.sql('SELECT count(*) AS n FROM wl_candidates WHERE selected=1 AND score<50')[0]['n'],4)
        self.assertEqual(self.store.sql('SELECT count(*) AS n FROM wl_candidates WHERE selected=1')[0]['n'],10)

    def test_pubmed_book_articles_are_not_lost(self):
        xml=b'<PubmedArticleSet><PubmedBookArticle><BookDocument><PMID>20301390</PMID><ArticleTitle>Rectal cancer</ArticleTitle><Book><BookTitle>Clinical reference</BookTitle><PubDate><Year>2026</Year></PubDate></Book></BookDocument></PubmedBookArticle></PubmedArticleSet>'
        source=p.PubMed()
        with patch.object(source,'call',return_value=xml):
            result=source.records(['20301390'])
        self.assertEqual(result[0]['pmid'],'20301390')
        self.assertEqual(result[0]['journal'],'Clinical reference')

    def test_frozen_config(self):
        self.source.fail=True
        with self.assertRaises(RuntimeError):
            p.run_etl(self.store,self.source,self.config,'2026-09-22')
        self.config['version']='new'
        with self.assertRaises(ValueError): p.run_etl(self.store,self.source,self.config,'2026-09-22')

    def test_materialized_run_keeps_frozen_hash_after_policy_release(self):
        p.run_etl(self.store,self.source,self.config,'2026-09-22')
        frozen=self.store.sql('SELECT config_hash FROM wl_runs')[0]['config_hash']
        self.config['version']='new'
        p.run_etl(self.store,self.source,self.config,'2026-09-22')
        self.assertEqual(self.store.sql('SELECT config_hash FROM wl_runs')[0]['config_hash'],frozen)

    def test_unlimited_selection_keeps_every_candidate_and_saved_decision(self):
        run='weekly-literature:2026-10-19:Asia/Shanghai'
        self.store.batch([('INSERT INTO wl_candidates VALUES(?,?,?,0)',(run,'PMID:'+str(i),50)) for i in range(157)])
        self.store.sql('UPDATE wl_candidates SET selected=1 WHERE evidence_key=?',('PMID:0',))
        self.store.sql('INSERT INTO wl_analysis VALUES(?,?,?)',(run,'PMID:0','{"decision":"exclude"}'))
        self.assertEqual(p.select_candidates(self.store,run,None)['selected'],157)
        self.assertEqual(p.select_candidates(self.store,run,None)['selected'],157)
        self.assertEqual(self.store.sql('SELECT payload FROM wl_analysis')[0]['payload'],'{"decision":"exclude"}')
    def test_historical_alias(self):
        self.store.sql('INSERT INTO wl_history VALUES(?,?)',('PMID:1','old'))
        p.run_etl(self.store,self.source,self.config,'2026-09-22')
        self.assertEqual(self.store.sql('SELECT * FROM wl_candidates'),[])
    def test_scope(self):
        cases = [
            ({'title':'Diabetes treatment randomized trial'},False),
            ({'title':'LARS unrelated protein study'},False),
            ({'title':'Machine learning for medical diagnosis'},False),
            ({'title':'Machine learning for medical diagnosis',
              'abstract':'External validation in a retrospective cohort of 300 patients.'},True),
            ({'title':'Shapley value estimation for multi-site data',
              'abstract':'Applications include biomedicine and environmental monitoring.'},False),
            ({'title':'A primer on evaluation methods for large language models in healthcare',
              'abstract':'A systematic review of clinical evaluation and patient safety.'},True),
            ({'title':'Rectal cancer shared decision making'},True),
            ({'title':'Peripheral vascular surgery'},False),
        ]
        for article,expected in cases:
            self.assertEqual(bool(p.score(article)),expected,article['title'])
        self.assertEqual(p.score({'title':'Rectal cancer','types':['Retracted Publication']}),0)

    def test_out_of_scope_records_do_not_consume_d1_rows(self):
        p.ingest_articles(self.store,'run',[
            dict(pmid='1',title='Generic transformer architecture',
                 abstract='Healthcare is a possible future application.',types=[]),
            dict(pmid='2',title='Deep learning for clinical diagnosis',
                 abstract='External validation in 120 patients.',types=[]),
        ])
        self.assertEqual([r['evidence_key'] for r in self.store.sql('SELECT evidence_key FROM wl_articles')],
                         ['PMID:2'])
        self.assertEqual(len(self.store.sql('SELECT * FROM wl_candidates')),1)
    def test_long_gap(self):
        p.run_etl(self.store,self.source,self.config,'2026-08-01')
        self.store.sql("UPDATE wl_runs SET status='PUBLISHED'")
        p.run_etl(self.store,self.source,self.config,'2026-09-22')
        row=self.store.sql('SELECT * FROM wl_runs ORDER BY run_key DESC')[0]
        self.assertEqual(row['window_start'],'2026-08-01')
    def test_date_split(self):
        class Limited:
            def search(self,q,start,end,field):
                if start!=end: raise OverflowError()
                return [start]
        ids=p.search_split(self.store,Limited(),'r','q','query','2026-09-01','2026-09-03','edat')
        self.assertEqual(len(ids),3)
    def test_budget_audit(self):
        self.config['max_candidates']=0
        p.run_etl(self.store,self.source,self.config,'2026-09-22')
        audit=json.loads(self.store.sql('SELECT audit FROM wl_runs')[0]['audit'])
        self.assertEqual(audit['deferred'],1)

    def test_generic_title_does_not_merge_distinct_dois(self):
        p.ingest_articles(self.store,'run',[
            dict(pmid='1',doi='10.1/first',title='Deep learning for medical diagnosis',abstract='Retrospective cohort of patients.'),
            dict(pmid='2',doi='10.1/second',title='Deep learning for medical diagnosis',abstract='Retrospective cohort of patients.')])
        self.assertEqual(len(self.store.sql('SELECT * FROM wl_articles')),2)
        p.ingest_articles(self.store,'run',[dict(pmid='2',doi='10.1/second',title='Deep learning for medical diagnosis',abstract='Retrospective cohort of patients.')])
        self.assertEqual(len(self.store.sql('SELECT * FROM wl_articles')),2)

    def test_strong_identifier_conflict_is_not_silently_merged(self):
        p.ingest_articles(self.store,'run',[
            dict(pmid='1',doi='10.1/first',title='Rectal cancer study one'),
            dict(pmid='2',doi='10.1/second',title='Rectal cancer study two')])
        with self.assertRaisesRegex(ValueError,'PMID:2'):
            p.ingest_articles(self.store,'run',[dict(pmid='2',doi='10.1/first',title='Rectal cancer study two')])

if __name__=='__main__': unittest.main()




