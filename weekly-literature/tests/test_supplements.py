import json
from pathlib import Path
import sys
import unittest
import urllib.parse
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from pipeline import Store, aliases, run_supplements
from supplements import EuropePMC, Arxiv, harvest

class SupplementTests(unittest.TestCase):
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



