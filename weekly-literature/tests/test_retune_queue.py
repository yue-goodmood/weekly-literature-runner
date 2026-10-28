import importlib.util
import json
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from pipeline import Store
from retune_queue import retune
from pipeline import select_candidates


class RetuneQueueTest(unittest.TestCase):
    def setUp(self):
        self.store=Store(':memory:'); self.store.migrate(); self.addCleanup(self.store.db.close)
        self.run='weekly-literature:2026-10-19:Asia/Shanghai'
        self.store.sql('INSERT INTO wl_runs(run_key,config_hash,window_start,window_end,status,audit) '
                       'VALUES(?,?,?,?,?,?)',(self.run,'frozen','2026-10-01','2026-10-18','READY','{}'))

    def add(self,key,title,abstract,analyzed=False):
        self.store.sql('INSERT INTO wl_articles VALUES(?,?)',
                       (key,json.dumps({'title':title,'abstract':abstract,'types':[]})))
        self.store.sql('INSERT INTO wl_candidates VALUES(?,?,?,1)',(self.run,key,60))
        if analyzed:
            self.store.sql('INSERT INTO wl_analysis VALUES(?,?,?)',(self.run,key,'{"decision":"include"}'))

    def test_prunes_only_pending_out_of_scope_and_is_idempotent(self):
        self.add('PMID:1','Generic transformer architecture','Healthcare may benefit.')
        self.add('PMID:2','Deep learning for gastric cancer diagnosis','External validation in 50 patients.')
        self.add('PMID:3','Generic transformer architecture','Healthcare may benefit.',True)
        result=retune(self.store,self.run)
        self.assertEqual(result['removed'],2)
        self.assertEqual(result['removed_analyzed'],1)
        self.assertEqual(result['selected'],1)
        self.assertEqual(result['analyzed'],0)
        self.assertEqual(result['pending'],1)
        self.assertEqual(retune(self.store,self.run),result)
        self.assertEqual({r['evidence_key'] for r in self.store.sql('SELECT evidence_key FROM wl_candidates')},
                         {'PMID:1','PMID:2','PMID:3'})
        self.assertEqual(self.store.sql('SELECT count(*) AS n FROM wl_analysis')[0]['n'],1)
        self.assertEqual({r['evidence_key'] for r in self.store.sql('SELECT evidence_key FROM wl_candidates WHERE selected=1')},{'PMID:2'})
        select_candidates(self.store,self.run,None)
        self.assertEqual({r['evidence_key'] for r in self.store.sql('SELECT evidence_key FROM wl_candidates WHERE selected=1')},{'PMID:2'})

    def test_scope_page_is_atomic_and_resume_keeps_saved_evidence(self):
        from unittest.mock import patch
        from retune_queue import PROGRESS
        for n in range(30): self.add('PMID:'+str(n).zfill(3),'Parkinson detection','50 patients',True)
        real=self.store.batch
        calls=[]
        def interrupted(statements):
            calls.append(len(statements))
            if len(calls)==2: raise TimeoutError('before second transaction')
            return real(statements)
        with patch.object(self.store,'batch',side_effect=interrupted):
            with self.assertRaises(TimeoutError): retune(self.store,self.run)
        self.assertTrue(all(n<=50 for n in calls))
        result=retune(self.store,self.run)
        self.assertEqual(result['removed'],30)
        self.assertEqual(result['selected'],0)
        self.assertEqual(self.store.sql('SELECT count(*) AS n FROM wl_analysis')[0]['n'],30)
        self.assertEqual(self.store.sql('SELECT count(*) AS n FROM wl_articles')[0]['n'],30)


if __name__=='__main__': unittest.main()



