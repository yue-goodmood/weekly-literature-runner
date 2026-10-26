import importlib.util
import json
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from pipeline import Store
from retune_queue import retune


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
        self.add('PMID:2','Deep learning for medical diagnosis','External validation in 50 patients.')
        self.add('PMID:3','Generic transformer architecture','Healthcare may benefit.',True)
        result=retune(self.store,self.run)
        self.assertEqual(result['removed'],1)
        self.assertEqual(result['selected'],2)
        self.assertEqual(result['analyzed'],1)
        self.assertEqual(result['pending'],1)
        self.assertEqual(retune(self.store,self.run),result)
        self.assertEqual({r['evidence_key'] for r in self.store.sql('SELECT evidence_key FROM wl_candidates')},
                         {'PMID:2','PMID:3'})


if __name__=='__main__': unittest.main()


