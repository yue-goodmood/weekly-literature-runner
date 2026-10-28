import importlib.util
import json
from pathlib import Path
import tempfile
import io
import urllib.error
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
    def test_cross_run_analysis_identity_lookup_uses_index_not_history_scan(self):
        plan=self.store.sql('EXPLAIN QUERY PLAN SELECT 1 FROM wl_analysis WHERE evidence_key=? LIMIT 1',('test',))
        detail=' '.join(row['detail'] for row in plan)
        self.assertIn('SEARCH',detail)
        self.assertIn('wl_analysis_by_evidence',detail)
        self.assertNotIn('SCAN',detail)
    def test_canonical_upgrade_never_rewrites_analyzed_evidence_or_blocks_ingestion(self):
        original={'source':'Europe PMC','source_id':'PPR:old','title':'Rectal cancer study',
                  'doi':'10.1/upgrade','abstract':'untrusted','types':['Preprint']}
        p.ingest_articles(self.store,'old-run',[original])
        evidence=self.store.sql('SELECT evidence_key FROM wl_articles')[0]['evidence_key']
        original_payload=self.store.sql('SELECT payload FROM wl_articles')[0]['payload']
        self.store.sql('INSERT INTO wl_analysis VALUES(?,?,?)',
                       ('old-run',evidence,'{"decision":"exclude","summary":"temporarily unverified"}'))
        incoming={'pmid':'55','title':'Rectal cancer study','doi':'10.1/upgrade',
                  'abstract':'Randomized trial in 40 patients','types':['Journal Article']}
        unrelated={'pmid':'66','title':'Rectal cancer surgery','abstract':'patient cohort','types':[]}
        p.ingest_articles(self.store,'new-run',[incoming,unrelated])
        p.ingest_articles(self.store,'new-run',[incoming,unrelated])
        self.assertEqual(self.store.sql('SELECT payload FROM wl_articles WHERE evidence_key=?',(evidence,))[0]['payload'],original_payload)
        self.assertEqual(len(self.store.sql('SELECT * FROM wl_analysis')),1)
        receipts=self.store.sql("SELECT payload FROM wl_checkpoints WHERE unit LIKE 'metadata:upgrade-pending:%'")
        self.assertEqual(len(receipts),1)
        self.assertEqual(json.loads(receipts[0]['payload'])['incoming']['pmid'],'55')
        self.assertEqual(len(self.store.sql("SELECT 1 FROM wl_articles WHERE evidence_key='PMID:66'")),1)
    def test_remote_row_budget_stops_before_next_call_without_discarding_result(self):
        with patch.dict(p.os.environ, {'CLOUDFLARE_ACCOUNT_ID':'example',
              'LITERATURE_D1_ID':'example','CLOUDFLARE_API_TOKEN':'test',
              'D1_RUN_READ_BUDGET':'10','D1_RUN_WRITE_BUDGET':'5'}):
            store = p.Store()
        result = {'success':True,'result':[{'success':True,'results':[{'ok':1}],
                    'meta':{'rows_read':10,'rows_written':2}}]}
        with patch.object(p, 'request', return_value=json.dumps(result).encode()) as send:
            self.assertEqual(store.sql('SELECT 1'), [{'ok':1}])
            self.assertRaises(p.DatabaseBudgetReached, store.sql, 'SELECT 2')
            self.assertEqual(send.call_count, 1)
        self.assertFalse(store.quota_exhausted)

    def test_d1_write_budget_counts_all_batch_statement_metadata(self):
        with patch.dict(p.os.environ, {'CLOUDFLARE_ACCOUNT_ID':'example',
              'LITERATURE_D1_ID':'example','CLOUDFLARE_API_TOKEN':'test',
              'D1_RUN_READ_BUDGET':'100','D1_RUN_WRITE_BUDGET':'5'}):
            store = p.Store()
        result = {'success':True,'result':[
            {'success':True,'results':[],'meta':{'rows_written':3}},
            {'success':True,'results':[],'meta':{'rows_written':2}}]}
        with patch.object(p, 'request', return_value=json.dumps(result).encode()) as send:
            store.batch([('INSERT 1',()),('INSERT 2',())])
            self.assertEqual(store.rows_written, 5)
            self.assertRaises(p.DatabaseBudgetReached, store.sql, 'SELECT 1')
            self.assertEqual(send.call_count, 1)

    def test_schema_manifest_avoids_repeated_ddl(self):
        with patch.object(self.store, 'sql', wraps=self.store.sql) as sql:
            self.store.migrate()
        self.assertEqual(sql.call_count, 2)
        self.assertTrue(all('SELECT' in call.args[0] for call in sql.call_args_list))

    def test_expired_budget_never_opens_connection_and_resets(self):
        token = p.DEADLINE.set(1)
        try:
            with patch.object(p.urllib.request, 'urlopen') as send:
                self.assertRaises(p.WorkBudgetReached, p.request, 'https://example.org')
                send.assert_not_called()
        finally:
            p.DEADLINE.reset(token)
        with self.assertRaises(p.WorkBudgetReached), p.work_budget(1):
            p.check_budget()
        self.assertIsNone(p.DEADLINE.get())

    def test_long_server_cooldown_never_sleeps_or_retries(self):
        error = urllib.error.HTTPError('https://example.org', 429, 'busy',
                                      {'Retry-After':'3600'}, io.BytesIO())
        with patch.object(p.urllib.request, 'urlopen', side_effect=error) as send, patch.object(p.time, 'sleep') as sleep:
            self.assertRaises(p.UpstreamDeferred, p.request, 'https://example.org')
            self.assertEqual(send.call_count, 1)
            sleep.assert_not_called()

    def test_retry_after_http_date_respects_actual_cooldown(self):
        import datetime
        now = datetime.datetime(2026, 10, 26, tzinfo=datetime.timezone.utc)
        with patch.object(p, 'dt', wraps=p.dt) as clock:
            clock.datetime.now.return_value = now
            self.assertRaises(p.UpstreamDeferred, p.retry_delay,
                              {'Retry-After':'Mon, 26 Oct 2026 01:00:00 GMT'}, 0)

    def test_transient_failure_has_three_attempt_ceiling(self):
        error = urllib.error.HTTPError('https://example.org', 503, 'busy', {}, io.BytesIO())
        with patch.object(p.urllib.request, 'urlopen', side_effect=error) as send, patch.object(p.time, 'sleep'):
            self.assertRaises(urllib.error.HTTPError, p.request, 'https://example.org')
            self.assertEqual(send.call_count, 3)

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
            def records(self,ids): return [dict(pmid=i,title=('Rectal cancer study ' if int(i)<10 else 'Machine learning for gastric diagnosis ')+i,abstract='retrospective cohort of 50 patients',types=[]) for i in ids]
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

    def test_frozen_config_is_saved_before_upstream_failure(self):
        self.source.fail=True
        with self.assertRaises(RuntimeError):
            p.run_etl(self.store,self.source,self.config,'2026-09-22')
        rows=self.store.sql('SELECT payload FROM wl_checkpoints WHERE unit=?',('etl:frozen-config:v1',))
        self.assertEqual(json.loads(rows[0]['payload']),self.config)

    def test_d1_http_daily_quota_never_retries(self):
        url='https://api.cloudflare.com/client/v4/accounts/example/d1/database/example/query'
        error=urllib.error.HTTPError(url,429,'quota',{},io.BytesIO(b'{"errors":[{"message":"free tier daily row read limit exceeded"}]}'))
        with patch.object(p.urllib.request,'urlopen',side_effect=error) as send, patch.object(p.time,'sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError,'D1_DAILY_QUOTA_EXCEEDED'):
                p.request(url)
            self.assertEqual(send.call_count,1)
            sleep.assert_not_called()

    def test_daily_quota_latches_across_reads_and_batches(self):
        with patch.dict(p.os.environ,{'CLOUDFLARE_ACCOUNT_ID':'example','LITERATURE_D1_ID':'example','CLOUDFLARE_API_TOKEN':'test'}):
            store=p.Store()
        failure=json.dumps({'success':False,'errors':[{'message':'daily row read limit exceeded'}]}).encode()
        with patch.object(p,'request',return_value=failure) as send:
            self.assertRaises(RuntimeError,store.sql,'SELECT 1')
            self.assertRaises(RuntimeError,store.sql,'SELECT 2')
            self.assertRaises(RuntimeError,store.batch,[('SELECT 3',())])
            self.assertEqual(send.call_count,1)

    def test_supplement_daily_quota_stops_without_audit_write(self):
        from unittest.mock import Mock
        fake=Mock()
        fake.sql.return_value=[{'start':None}]
        config={'supplementary_sources':{'Europe PMC':{'enabled':True,'queries':{},'max_pages':None},'arXiv':{'enabled':True,'queries':{},'max_pages':None}}}
        with patch('supplements.harvest',side_effect=RuntimeError('D1_DAILY_QUOTA_EXCEEDED')) as harvest:
            self.assertRaises(RuntimeError,p.run_supplements,fake,'run',config,
                {'window_start':'2026-10-01','window_end':'2026-10-18'}, {'Europe PMC':object(),'arXiv':object()})
            self.assertEqual(harvest.call_count,1)
            self.assertEqual(fake.sql.call_count,1)  # Initial gap read only.

    def test_retry_after_date_does_not_break_transient_retry(self):
        from unittest.mock import MagicMock
        error=urllib.error.HTTPError('https://example.org',503,'busy',{'Retry-After':'Wed, 21 Oct 2026 07:28:00 GMT'},io.BytesIO())
        response=MagicMock()
        response.__enter__.return_value.read.return_value=b'ok'
        with patch.object(p.urllib.request,'urlopen',side_effect=[error,response]), patch.object(p.time,'sleep'):
            self.assertEqual(p.request('https://example.org'),b'ok')

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
              'abstract':'External validation in a retrospective cohort of 300 patients.'},False),
            ({'title':'Shapley value estimation for multi-site data',
              'abstract':'Applications include biomedicine and environmental monitoring.'},False),
            ({'title':'A primer on evaluation methods for large language models in healthcare',
              'abstract':'A systematic review of clinical evaluation and patient safety.'},False),
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
            dict(pmid='2',title='Deep learning for gastric diagnosis',
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
            dict(pmid='1',doi='10.1/first',title='Deep learning for medical diagnosis',abstract='Retrospective gastric cohort of gastric cancer patients.'),
            dict(pmid='2',doi='10.1/second',title='Deep learning for medical diagnosis',abstract='Retrospective gastric cohort of gastric cancer patients.')])
        self.assertEqual(len(self.store.sql('SELECT * FROM wl_articles')),2)
        p.ingest_articles(self.store,'run',[dict(pmid='2',doi='10.1/second',title='Deep learning for medical diagnosis',abstract='Retrospective gastric cohort of gastric cancer patients.')])
        self.assertEqual(len(self.store.sql('SELECT * FROM wl_articles')),2)

    def test_digestive_pelvic_scope_is_not_generic_medical_ai(self):
        for title in ('Gastric cancer surgery','Small bowel obstruction','Crohn disease',
                      'Pelvic floor dyssynergia','Fecal incontinence','Functional constipation',
                      'Esophageal achalasia','Pancreatic cancer','Liver fibrosis','Rectal prolapse',
                      'Screening colonoscopy','Diagnostic gastroscopy','Device assisted enteroscopy'):
            self.assertGreater(p.score({'title':title}),0,title)
        for title in ('Deep learning for Parkinson disease','AI in acute myeloid leukemia',
                      'Psychotic disorders and speech compression','Kidney stone federated learning'):
            self.assertEqual(p.score({'title':title,'abstract':'External validation in 500 patients.'}),0,title)
        self.assertEqual(p.score({'title':'Generic model architecture','abstract':'Potential colorectal applications.'}),0)
        self.assertGreater(p.score({'title':'Constipation in Parkinson disease'}),0)

    def test_strong_identifier_conflict_is_not_silently_merged(self):
        p.ingest_articles(self.store,'run',[
            dict(pmid='1',doi='10.1/first',title='Rectal cancer study one'),
            dict(pmid='2',doi='10.1/second',title='Rectal cancer study two')])
        with self.assertRaisesRegex(ValueError,'PMID:2'):
            p.ingest_articles(self.store,'run',[dict(pmid='2',doi='10.1/first',title='Rectal cancer study two')])

if __name__=='__main__': unittest.main()





