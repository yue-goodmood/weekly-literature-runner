import importlib.util
import json
import sys
from pathlib import Path
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
import pipeline
import publish
import report_format
import recover

class PublicationTest(unittest.TestCase):
    def setUp(self):
        self.store=pipeline.Store(':memory:');self.store.migrate();self.addCleanup(self.store.db.close);self.run='weekly-literature:2026-09-22:Asia/Shanghai'
        verifier=patch.object(publish,'verify_artifacts');verifier.start();self.addCleanup(verifier.stop)
        self.store.sql('INSERT INTO wl_runs VALUES(?,?,?,?,?,?)',(self.run,'h','2026-09-01','2026-09-21','READY','{}'))
        self.store.sql('INSERT INTO wl_articles VALUES(?,?)',('PMID:1',json.dumps({'title':'Rectal cancer'})))
        self.store.sql('INSERT INTO wl_candidates VALUES(?,?,?,?)',(self.run,'PMID:1',50,1))
        self.store.sql('INSERT INTO wl_publications(run_key,page_id) VALUES(?,?)',(self.run,'page'))
        self.blocks=[];self.writes=0;self.fail=False
    def read(self,*args,**kwargs):
        return json.dumps({'results':self.blocks,'has_more':False}).encode()
    def write(self,req,**kwargs):
        self.writes+=1
        self.blocks.extend(json.loads(req.data)['children'])
        if self.fail: raise TimeoutError('response lost after server committed')
        class Response:
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def read(self): return b'{}'
        return Response()
    def analysis(self):
        self.store.sql('INSERT INTO wl_analysis VALUES(?,?,?)',(self.run,'PMID:1',json.dumps({'decision':'include','summary':'Finding','popscience':'Angle'})))
    def test_missing_analysis_blocks_publication(self):
        with self.assertRaises(ValueError):publish.publish(self.store,self.run)
        self.assertEqual(self.store.sql('SELECT * FROM wl_history'),[])

    def test_scoped_final_promotes_api_pointer_and_preserves_legacy_ledger(self):
        self.analysis()
        self.store.sql('UPDATE wl_runs SET audit=?',(json.dumps({'triage':{'policy':pipeline.TRIAGE_POLICY}}),))
        self.store.sql('INSERT INTO wl_history VALUES(?,?)',('historical-other-specialty',self.run))
        def provision(store,artifact):
            store.sql('INSERT INTO wl_publications(run_key,page_id) VALUES(?,?)',(artifact,'scoped-page'))
            return 'scoped-page'
        def verified(store,artifact,page,blocks,token):
            store.sql("UPDATE wl_publications SET status='VERIFIED',content_hash='checked' WHERE run_key=?",(artifact,))
        with patch.dict('os.environ',{'NOTION_TOKEN':'test','NOTION_DATA_SOURCE_ID':'db'}), \
             patch.object(publish,'provision_page',side_effect=provision), \
             patch.object(publish,'publish_report_blocks',side_effect=verified), \
             patch.object(publish,'update_report_metadata'):
            publish.publish(self.store,self.run)
            publish.publish(self.store,self.run)
        self.assertEqual(self.store.sql('SELECT page_id FROM wl_publications WHERE run_key=?',(self.run,))[0]['page_id'],'scoped-page')
        self.assertEqual(self.store.sql('SELECT page_id FROM wl_publications WHERE run_key=?',('legacy:'+self.run+':pre-digestive-scope',))[0]['page_id'],'page')
        self.assertEqual(self.store.sql('SELECT count(*) AS n FROM wl_history')[0]['n'],2)
        self.assertEqual(self.store.sql('SELECT count(*) AS n FROM wl_analysis')[0]['n'],1)

    def test_missing_metadata_cannot_disappear_in_join(self):
        self.analysis()
        self.store.sql('DELETE FROM wl_articles')
        with self.assertRaisesRegex(ValueError,'metadata missing'):
            publish.publish(self.store,self.run)
        self.assertEqual(self.store.sql('SELECT status FROM wl_runs')[0]['status'],'READY')

    def test_history_owner_conflict_prevents_complete(self):
        self.analysis()
        self.store.sql('INSERT INTO wl_history VALUES(?,?)',('PMID:1','older-run'))
        with patch.dict('os.environ',{'NOTION_TOKEN':'test','NOTION_DATA_SOURCE_ID':'db'}),patch.object(report_format,'request',self.read),patch.object(publish,'update_report_metadata'),patch('urllib.request.urlopen',self.write):
            with self.assertRaisesRegex(ValueError,'history differs'):
                publish.publish(self.store,self.run)
        self.assertEqual(self.store.sql('SELECT status FROM wl_runs')[0]['status'],'PUBLISHING')

    def test_failed_independent_verification_does_not_commit_history(self):
        self.analysis()
        publish.verify_artifacts.side_effect=ValueError('Independent readback failed')
        with patch.dict('os.environ',{'NOTION_TOKEN':'test','NOTION_DATA_SOURCE_ID':'db'}),patch.object(report_format,'request',self.read),patch.object(publish,'update_report_metadata') as metadata,patch('urllib.request.urlopen',self.write):
            with self.assertRaisesRegex(ValueError,'Independent readback'):
                publish.publish(self.store,self.run)
            metadata.assert_not_called()
        self.assertEqual(self.store.sql('SELECT * FROM wl_history'),[])
    def test_ambiguous_append_resumes_without_duplicates(self):
        self.analysis();self.fail=True
        with patch.dict('os.environ',{'NOTION_TOKEN':'test','NOTION_DATA_SOURCE_ID':'db'}),patch.object(report_format,'request',self.read),patch.object(publish,'update_report_metadata'),patch('urllib.request.urlopen',self.write):
            with self.assertRaises(TimeoutError): publish.publish(self.store,self.run)
            self.assertEqual(self.store.sql('SELECT * FROM wl_history'),[])
            self.fail=False;publish.publish(self.store,self.run)
            self.assertEqual(self.writes,1)
            publish.publish(self.store,self.run)
            self.assertEqual(self.writes,1)
            self.assertEqual(self.store.sql('SELECT status FROM wl_runs')[0]['status'],'PUBLISHED')
    def test_preserves_user_edits(self):
        self.analysis();self.blocks=[{'object':'block','type':'paragraph','paragraph':{'rich_text':[{'plain_text':'user text'}]}}]
        with patch.dict('os.environ',{'NOTION_TOKEN':'test','NOTION_DATA_SOURCE_ID':'db'}),patch.object(report_format,'request',self.read):
            with self.assertRaises(ValueError):publish.publish(self.store,self.run)
        self.assertEqual(self.store.sql('SELECT * FROM wl_history'),[])

    def test_reopened_report_appends_only_new_decisions_and_resumes_timeout(self):
        self.analysis()
        with patch.dict('os.environ',{'NOTION_TOKEN':'test','NOTION_DATA_SOURCE_ID':'db'}),patch.object(report_format,'request',self.read),patch.object(publish,'update_report_metadata'),patch('urllib.request.urlopen',self.write):
            publish.publish(self.store,self.run)
            original=list(self.blocks)
            base={'blocks':[report_format._canonical(b) for b in original],'evidence_keys':['PMID:1']}
            self.store.sql('INSERT INTO wl_checkpoints VALUES(?,?,?)',(self.run,recover.BASE,json.dumps(base)))
            self.store.sql('INSERT INTO wl_articles VALUES(?,?)',('PMID:2',json.dumps({'title':'New rectal cancer study'})))
            self.store.sql('INSERT INTO wl_candidates VALUES(?,?,?,?)',(self.run,'PMID:2',50,0))
            opened=recover.reopen(self.store,self.run)
            self.assertEqual(opened['remaining'],1)
            self.assertEqual(recover.reopen(self.store,self.run)['remaining'],1)
            self.assertEqual(self.store.sql('SELECT count(*) n FROM wl_analysis')[0]['n'],1)
            self.store.sql('INSERT INTO wl_analysis VALUES(?,?,?)',(self.run,'PMID:2',json.dumps({'decision':'include','summary':'New finding','popscience':'Angle'})))
            self.fail=True
            with self.assertRaises(TimeoutError):publish.publish(self.store,self.run)
            self.assertEqual(self.blocks[:len(original)],original)
            self.fail=False
            publish.publish(self.store,self.run)
            writes=self.writes
            publish.publish(self.store,self.run)
            self.assertEqual(writes,self.writes)
            self.assertEqual(self.store.sql('SELECT count(*) n FROM wl_history')[0]['n'],2)

    def test_unlimited_publication_cannot_ignore_unqueued_candidates(self):
        self.analysis()
        self.store.sql('UPDATE wl_runs SET audit=?',(json.dumps({'review_policy':'unlimited-v1'}),))
        self.store.sql('INSERT INTO wl_candidates VALUES(?,?,?,0)',(self.run,'PMID:2',50))
        with self.assertRaisesRegex(ValueError,'Unqueued'):
            publish.publish(self.store,self.run)
    def test_excluded_paper_is_not_published_history(self):
        self.analysis()
        self.store.sql('INSERT INTO wl_articles VALUES(?,?)',('PMID:2',json.dumps({'title':'Unrelated'})))
        self.store.sql('INSERT INTO wl_candidates VALUES(?,?,?,?)',(self.run,'PMID:2',50,1))
        self.store.sql('INSERT INTO wl_analysis VALUES(?,?,?)',(self.run,'PMID:2',json.dumps({'decision':'exclude','summary':'Out of scope','popscience':''})))
        with patch.dict('os.environ',{'NOTION_TOKEN':'test','NOTION_DATA_SOURCE_ID':'db'}),patch.object(report_format,'request',self.read),patch.object(publish,'update_report_metadata'),patch('urllib.request.urlopen',self.write):
            publish.publish(self.store,self.run)
        self.assertEqual([r['evidence_key'] for r in self.store.sql('SELECT * FROM wl_history')],['PMID:1'])
        self.assertNotIn('Unrelated',json.dumps(self.blocks))

class ProvisionTest(unittest.TestCase):
    def setUp(self):
        self.store=pipeline.Store(':memory:');self.store.migrate(); self.addCleanup(self.store.db.close)
    def test_unknown_creation_is_not_retried(self):
        self.store.sql('INSERT INTO wl_page_intents VALUES(?)',('run',))
        replies=[b'{"properties":{"Name":{"type":"title"}}}',b'{"results":[],"has_more":false}']
        with patch.dict('os.environ',{'NOTION_TOKEN':'test','NOTION_DATA_SOURCE_ID':'db'}),patch.object(publish,'request',side_effect=replies),patch('urllib.request.urlopen') as write:
            with self.assertRaises(ValueError):publish.provision_page(self.store,'run')
            write.assert_not_called()
    def test_lost_create_response_reconciles_existing_page(self):
        self.store.sql('INSERT INTO wl_page_intents VALUES(?)',('run',))
        replies=[b'{"properties":{"Name":{"type":"title"}}}',b'{"results":[{"id":"page"}],"has_more":false}']
        with patch.dict('os.environ',{'NOTION_TOKEN':'test','NOTION_DATA_SOURCE_ID':'db'}),patch.object(publish,'request',side_effect=replies),patch('urllib.request.urlopen') as write:
            self.assertEqual(publish.provision_page(self.store,'run'),'page')
            write.assert_not_called()

class ArticlePublicationTests(unittest.TestCase):
    def setUp(self):
        for name in ('verify_artifacts','finalize_article_title'):
            patcher=patch.object(publish,name);patcher.start();self.addCleanup(patcher.stop)
    def test_stale_source_pages_get_one_fresh_generation_not_infinite_retry(self):
        store=pipeline.Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        run='weekly-literature:2026-10-19:Asia/Shanghai'
        audit={'supplementary_sources':{'Europe PMC':{'status':'PARTIAL','window_start':'2026-09-06','window_end':'2026-10-18'}}}
        store.sql('INSERT INTO wl_runs VALUES(?,?,?,?,?,?)',(run,'h','2026-09-28','2026-10-18','READY',json.dumps(audit)))
        with patch('supplements.harvest',side_effect=[ValueError('Repeated source record during pagination'),ValueError('Repeated source record during pagination')]) as harvest:
            result=recover.repair_supplements(store,run)
            self.assertEqual(result['status'],'PARTIAL')
            self.assertEqual(harvest.call_count,2)
            self.assertEqual(harvest.call_args.kwargs['namespace'],'recovery-v1:')
        with patch('supplements.harvest',side_effect=ValueError('Repeated source record during pagination')) as harvest:
            recover.repair_supplements(store,run)
            self.assertEqual(harvest.call_count,1)

    def test_failure_does_not_prevent_other_articles_and_recovery_reuses_weekly(self):
        store=pipeline.Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        run='weekly-literature:2026-09-22:Asia/Shanghai'
        store.sql('INSERT INTO wl_runs VALUES(?,?,?,?,?,?)',(run,'h','2026-09-01','2026-09-21','READY','{}'))
        for key in ('PMID:1','PMID:2'):
            store.sql('INSERT INTO wl_articles VALUES(?,?)',(key,json.dumps({'title':'Rectal cancer'})))
            store.sql('INSERT INTO wl_candidates VALUES(?,?,?,?)',(run,key,50,1))
            payload={'decision':'include','summary':'Finding','popscience':'Angle','popscience_priority':'high','popscience_article':{'title':'Patient question','body':'Evidence-based explanation','references':['https://example.org/paper']}}
            store.sql('INSERT INTO wl_analysis VALUES(?,?,?)',(run,key,json.dumps(payload)))
        published=[]
        def output(store,key,page,text):
            published.append(key)
            if key=='popscience:PMID:1':raise TimeoutError()
        with patch.dict('os.environ',{'NOTION_TOKEN':'test','NOTION_DATA_SOURCE_ID':'db','NOTION_ARTICLE_DATA_SOURCE_ID':'articles'}),patch.object(publish,'provision_page',return_value='page'),patch.object(publish,'publish_report_blocks'),patch.object(publish,'update_report_metadata'),patch.object(publish,'publish_text',side_effect=output):
            with self.assertRaises(ValueError):publish.publish(store,run)
        self.assertIn('popscience:PMID:2',published)
        self.assertEqual(store.sql('SELECT * FROM wl_history'),[])
        self.assertEqual(store.sql('SELECT status FROM wl_runs')[0]['status'],'PUBLISHING')
        with patch.dict('os.environ',{'NOTION_TOKEN':'test','NOTION_DATA_SOURCE_ID':'db','NOTION_ARTICLE_DATA_SOURCE_ID':'articles'}),patch.object(publish,'provision_page',return_value='page'),patch.object(publish,'publish_report_blocks'),patch.object(publish,'update_report_metadata'),patch.object(publish,'publish_text'):
            publish.publish(store,run)
        self.assertEqual(store.sql('SELECT status FROM wl_runs')[0]['status'],'PUBLISHED')

class FinalVerificationTests(unittest.TestCase):
    def test_missing_required_artifact_blocks_final_state(self):
        with patch('verify_publication.verify',return_value=[{'artifact':'run'}]):
            with self.assertRaisesRegex(ValueError,'publication missing'):
                publish.verify_artifacts(None,'run',{'run','popscience:PMID:1'})

    def test_article_title_finalization_reads_back_and_preserves_manual_titles(self):
        def page(title):return json.dumps({'properties':{'Name':{'type':'title','title':[{'plain_text':title}]}}}).encode()
        class Response:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read(self):return b'{}'
        with patch.dict('os.environ',{'NOTION_TOKEN':'test'}),patch.object(publish,'request',side_effect=[page('popscience:PMID:1'),page('患者的问题')]),patch('urllib.request.urlopen',return_value=Response()) as write:
            publish.finalize_article_title('page','popscience:PMID:1','患者的问题')
            self.assertEqual(write.call_count,1)
        with patch.dict('os.environ',{'NOTION_TOKEN':'test'}),patch.object(publish,'request',return_value=page('用户的新题目')),patch('urllib.request.urlopen') as write:
            with self.assertRaisesRegex(ValueError,'preserve user'):
                publish.finalize_article_title('page','popscience:PMID:1','患者的问题')
            write.assert_not_called()




