import json
from pathlib import Path
import sys
import unittest
import urllib.parse
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from pipeline import Store, aliases, run_supplements
from supplements import EuropePMC, Arxiv, harvest, harvest_windowed, SourceSnapshotChanged

class SupplementTests(unittest.TestCase):
    def test_resume_after_39_pages_reads_only_next_page(self):
        from unittest.mock import patch
        store=Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            fail=True
            def page(self,q,start,end,cursor):
                offset=int(cursor or 0)
                if offset==39 and self.fail: raise TimeoutError()
                return {'records':[{'source_id':str(offset)}],'total':40,
                        'next':str(offset+1) if offset<39 else None}
        adapter=Adapter(); delivered=[]
        with self.assertRaises(TimeoutError):
            harvest(store,'r',adapter,{'q':'x'},'a','b',delivered.append)
        self.assertEqual(len(delivered),39)
        adapter.fail=False
        with patch.object(store,'sql',wraps=store.sql) as sql:
            value=harvest(store,'r',adapter,{'q':'x'},'a','b',delivered.append)
            self.assertLessEqual(sql.call_count,8)
        self.assertEqual(value['counts'],{'q':40})
        self.assertEqual([r['source_id'] for r in delivered],[str(i) for i in range(40)])

    def test_cross_page_duplicate_is_detected_after_resume(self):
        store=Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            fail=True
            def page(self,q,start,end,cursor):
                if cursor=='last' and self.fail: raise TimeoutError()
                return ({'records':[{'source_id':'a'}],'total':3,'next':'middle'} if cursor is None else
                        {'records':[{'source_id':'b'}],'total':3,'next':'last'} if cursor=='middle' else
                        {'records':[{'source_id':'a'}],'total':3,'next':None})
        adapter=Adapter()
        with self.assertRaises(TimeoutError): harvest(store,'r',adapter,{'q':'x'},'a','b',lambda row:None)
        adapter.fail=False
        with self.assertRaisesRegex(ValueError,'Repeated source record'):
            harvest(store,'r',adapter,{'q':'x'},'a','b',lambda row:None)

    def test_cursor_cycle_survives_process_resume(self):
        store=Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            fail=True
            def page(self,q,start,end,cursor):
                if cursor=='two' and self.fail: raise TimeoutError()
                following={None:'one','one':'two','two':'one'}[cursor]
                return {'records':[{'source_id':str(cursor)}],'total':5,'next':following}
        adapter=Adapter()
        with self.assertRaises(TimeoutError): harvest(store,'r',adapter,{'q':'x'},'a','b',lambda row:None)
        adapter.fail=False
        with self.assertRaisesRegex(ValueError,'repeated source cursor'):
            harvest(store,'r',adapter,{'q':'x'},'a','b',lambda row:None)

    def test_page_marker_seen_ids_and_pointer_rollback_together(self):
        from unittest.mock import patch
        store=Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            calls=0
            def page(self,*args):
                self.calls+=1
                return {'records':[{'source_id':'a'}],'total':1,'next':None}
        adapter=Adapter(); real_batch=store.batch
        def broken(statements):
            real_batch(statements[:1]+[('INVALID SQL',())]+statements[1:])
        with patch.object(store,'batch',side_effect=broken):
            with self.assertRaises(Exception): harvest(store,'r',adapter,{'q':'x'},'a','b',lambda row:None)
        self.assertEqual(store.sql('SELECT * FROM wl_harvest_seen'),[])
        self.assertEqual(store.sql("SELECT 1 FROM wl_checkpoints WHERE unit LIKE '%:ingested' OR unit LIKE '%:progress:%'"),[])
        self.assertEqual(harvest(store,'r',adapter,{'q':'x'},'a','b',lambda row:None)['counts'],{'q':1})
        self.assertEqual(adapter.calls,1)

    def test_legacy_cached_page_migrates_without_reingestion(self):
        from pipeline import digest
        store=Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        unit='supplement:test:'+digest(['q','x','a','b',None])
        page={'records':[{'source_id':'a'}],'total':2,'next':'b'}
        store.sql('INSERT INTO wl_checkpoints VALUES(?,?,?)',('r',unit,json.dumps(page)))
        store.sql('INSERT INTO wl_checkpoints VALUES(?,?,?)',('r',unit+':ingested','true'))
        class Adapter:
            name='test'
            supports_date_slicing=True
            def page(self,q,start,end,cursor):
                self_cursor=cursor
                if self_cursor!='b': raise AssertionError('legacy first page fetched again')
                return {'records':[{'source_id':'b'}],'total':2,'next':None}
        delivered=[]
        harvest_windowed(store,'r',Adapter(),{'q':'x'},'a','b',delivered.append)
        self.assertEqual(delivered,[{'source_id':'b'}])
        self.assertEqual(len(store.sql("SELECT value FROM wl_harvest_seen WHERE value LIKE 'id:%'")),2)

    def test_fresh_window_plan_is_disjoint_complete_and_resumable(self):
        store=Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            supports_date_slicing=True
            fail=True
            windows=[]
            def page(self,q,start,end,cursor):
                self.windows.append((start,end))
                if start=='2026-01-08' and self.fail: raise TimeoutError()
                return {'records':[{'source_id':start}],'total':1,'next':None}
        adapter=Adapter()
        with self.assertRaises(TimeoutError):
            harvest_windowed(store,'r',adapter,{'q':'x'},'2026-01-01','2026-01-16',lambda r:None)
        adapter.fail=False
        value=harvest_windowed(store,'r',adapter,{'q':'x'},'2026-01-01','2026-01-16',lambda r:None)
        self.assertEqual(value['counts'],{'q':3})
        self.assertEqual(adapter.windows.count(('2026-01-01','2026-01-07')),1)
        parts=value['partitions']['q']
        self.assertEqual([(p['window_start'],p['window_end']) for p in parts],
                         [('2026-01-01','2026-01-07'),('2026-01-08','2026-01-14'),('2026-01-15','2026-01-16')])
        self.assertTrue(all(p['status']=='COMPLETE_FOR_QUERY' for p in parts))

    def test_live_count_drift_retires_only_query_snapshot_and_resumes(self):
        store=Store(':memory:'); store.migrate(); self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            changed=False
            calls=[]
            def page(self,q,start,end,cursor):
                self.calls.append(cursor)
                if not self.changed:
                    return {'records':[{'source_id':cursor or 'a'}],
                            'total':2 if cursor is None else 3,'next':'b'}
                return {'records':[{'source_id':x} for x in ('a','b','c')],
                        'total':3,'next':None}
        adapter=Adapter(); delivered=[]
        with self.assertRaises(SourceSnapshotChanged):
            harvest(store,'r',adapter,{'q':'x'},'a','b',delivered.append)
        self.assertEqual([x['source_id'] for x in delivered],['a'])
        self.assertEqual(store.sql("SELECT 1 FROM wl_checkpoints WHERE unit LIKE '%supplement-complete:%' AND unit NOT LIKE '%:snapshot' AND unit NOT LIKE '%:progress:%'"),[])
        old=store.sql("SELECT unit,payload FROM wl_checkpoints WHERE unit LIKE 'supplement:%'")
        adapter.changed=True
        result=harvest(store,'r',adapter,{'q':'x'},'a','b',delivered.append)
        self.assertEqual(result['counts'],{'q':3})
        self.assertEqual(adapter.calls,[None,'b',None])
        self.assertEqual(store.sql("SELECT unit,payload FROM wl_checkpoints WHERE unit LIKE 'supplement:%'"),old)
        self.assertEqual(len(store.sql("SELECT 1 FROM wl_checkpoints WHERE unit LIKE 'snapshot:1:supplement:%'")),2)

    def test_completed_query_resume_is_one_read_not_full_page_replay(self):
        from unittest.mock import patch
        store=Store(':memory:'); store.migrate(); self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            calls=0
            def page(self,q,start,end,cursor):
                self.calls += 1
                return {'records':[{'source_id':'one'}], 'total':1, 'next':None}
        adapter=Adapter()
        first=harvest(store,'r',adapter,{'q':'x'},'a','b',lambda item:None)
        with patch.object(store, 'sql', wraps=store.sql) as sql:
            second=harvest(store,'r',adapter,{'q':'x'},'a','b',lambda item:None)
            self.assertEqual(sql.call_count, 1)
            self.assertTrue(sql.call_args.args[0].startswith('SELECT'))
        self.assertEqual(first,second)
        self.assertEqual(adapter.calls,1)

    def test_completed_query_cache_does_not_cross_windows_or_namespaces(self):
        store=Store(':memory:'); store.migrate(); self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            calls=0
            def page(self,*args):
                self.calls += 1
                return {'records':[], 'total':0, 'next':None}
        adapter=Adapter()
        for start,namespace in [('a',''),('c',''),('a','new:')]:
            harvest(store,'r',adapter,{'q':'x'},start,'z',lambda item:None,namespace=namespace)
        self.assertEqual(adapter.calls,3)

    def test_partial_query_never_gets_complete_receipt(self):
        store=Store(':memory:'); store.migrate(); self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            def page(self,*args):
                return {'records':[], 'total':2, 'next':None}
        self.assertRaises(ValueError,harvest,store,'r',Adapter(),{'q':'x'},'a','b',lambda item:None)
        self.assertEqual(store.sql("SELECT 1 FROM wl_checkpoints WHERE unit LIKE '%supplement-complete:%'"),[])

    def test_europe_pmc_sort_is_explicit_and_new_cache_cannot_mix_legacy_pages(self):
        urls=[]
        def transport(url):
            urls.append(url)
            return b'{"hitCount":0,"resultList":{"result":[]}}'
        adapter=EuropePMC(transport)
        store=Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        result=harvest(store,'r',adapter,{'q':'clinical'},'2026-09-01','2026-09-02',lambda row:None)
        self.assertEqual(urllib.parse.parse_qs(urllib.parse.urlsplit(urls[0]).query)['sort'],['P_PDATE_D asc'])
        self.assertEqual(result['checkpoint_namespace'],'epmc-date-v2:')
    def test_unlimited_harvest_continues_after_thirtieth_page(self):
        store=Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            def page(self,q,start,end,cursor):
                offset=int(cursor or 0)
                return {'records':[{'source_id':str(offset)}],'total':33,'next':str(offset+1) if offset<32 else None}
        result=harvest(store,'r',Adapter(),{'q':'x'},'a','b',lambda item:None,None)
        self.assertEqual(result['counts']['q'],33)

    def test_explicit_page_budget_remains_partial(self):
        store=Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            def page(self,q,start,end,cursor):
                offset=int(cursor or 0)
                return {'records':[{'source_id':str(offset)}],'total':33,'next':str(offset+1)}
        with self.assertRaisesRegex(ValueError,'page budget'):
            harvest(store,'r',Adapter(),{'q':'x'},'a','b',lambda item:None,30)
    def test_europe_pmc_preserves_preprint_and_abstract(self):
        adapter=EuropePMC(lambda url: json.dumps({'hitCount':1,'resultList':{'result':[{'id':'PPR1','source':'PPR','title':'Clinical AI','abstractText':'<p>Result &amp; limitation</p>'}]}}).encode())
        page=adapter.page('clinical','2026-09-01','2026-09-21',None)
        self.assertTrue(page['records'][0]['preprint'])
        self.assertEqual(page['records'][0]['abstract'],'Result & limitation')
        self.assertEqual(page['total'],1)
    def test_arxiv_version_alias_and_provenance(self):
        xml=b'''<feed xmlns="http://www.w3.org/2005/Atom" xmlns:o="http://a9.com/-/spec/opensearch/1.1/"><o:totalResults>1</o:totalResults><entry><id>http://arxiv.org/abs/2609.12345v2</id><title>Medical AI</title><summary>clinical testing</summary><published>2026-09-01</published><updated>2026-09-21</updated></entry></feed>'''
        article=Arxiv(lambda url:xml,lambda delay:None).page('medical','2026-09-01','2026-09-21',None)['records'][0]
        self.assertIn('ARXIV:2609.12345',aliases(article));self.assertTrue(article['preprint'])
    def test_pagination_resume_uses_cached_successful_pages(self):
        store=Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            calls=[]
            fail=True
            def page(self,q,start,end,cursor):
                self.calls.append(cursor)
                if cursor and self.fail:raise TimeoutError()
                return {'records':[{'source_id':cursor or 'first'}],'total':2,'next':'second' if cursor is None else None}
        adapter=Adapter();seen=[]
        with self.assertRaises(TimeoutError):harvest(store,'run',adapter,{'q':'x'},'2026-01-01','2026-01-02',seen.append)
        adapter.fail=False
        result=harvest(store,'run',adapter,{'q':'x'},'2026-01-01','2026-01-02',seen.append)
        self.assertEqual(result['status'],'COMPLETE_FOR_QUERY');self.assertEqual(adapter.calls.count(None),1)
    def test_repeated_cursor_fails(self):
        store=Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            def page(self,*args):return {'records':[{'source_id':'same'}],'total':3,'next':'repeat'}
        with self.assertRaises(ValueError):harvest(store,'r',Adapter(),{'q':'x'},'a','b',lambda article:None)
    def test_failed_source_gap_is_carried(self):
        store=Store(':memory:');store.migrate();self.addCleanup(store.db.close)
        class Adapter:
            name='test'
            def page(self,*args):raise TimeoutError()
        config={'supplementary_sources':{'test':{'enabled':True,'queries':{'q':'x'},'max_pages':2}}}
        audit=run_supplements(store,'run',config,{'window_start':'2026-08-01','window_end':'2026-08-21'},{'test':Adapter()})
        self.assertEqual(audit['test']['status'],'PARTIAL')
        audit=run_supplements(store,'run2',config,{'window_start':'2026-09-01','window_end':'2026-09-21'},{'test':Adapter()})
        self.assertEqual(audit['test']['window_start'],'2026-08-01')




