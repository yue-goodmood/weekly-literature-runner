import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from pipeline import Store
from publish import publish
from review_corrections import apply_correction


class ReviewCorrectionTest(unittest.TestCase):
    def setUp(self):
        self.store = Store(':memory:')
        self.addCleanup(self.store.db.close)
        self.store.migrate()
        self.run = 'weekly-literature:2026-01-01:Asia/Shanghai'
        self.key = 'DOI:synthetic'
        original = json.dumps({'decision':'exclude','summary':'No verified source'})
        self.store.sql('INSERT INTO wl_runs VALUES(?,?,?,?,?,?)',(self.run,'hash','start','end','READY','{}'))
        self.store.sql('INSERT INTO wl_articles VALUES(?,?)',(self.key,json.dumps({'title':'Fixture paper'})))
        self.store.sql('INSERT INTO wl_candidates VALUES(?,?,0,1)',(self.run,self.key))
        self.store.sql('INSERT INTO wl_analysis VALUES(?,?,?)',(self.run,self.key,original))
        self.item = dict(evidence_key=self.key, original_payload=original,
                         source_url='https://example.org/paper', source_title='Fixture paper',
                         reason='Verified original abstract', analysis=dict(decision='include',
                         summary='中文题名：示例', popscience='示例', popscience_priority='low'))

    def test_replay_conflict_and_final_delivery(self):
        for _ in range(2):
            apply_correction(self.store,self.run,self.item)
        with self.assertRaises(ValueError):
            apply_correction(self.store,self.run,{**self.item,'reason':'conflict'})
        self.assertEqual(self.store.sql('SELECT payload FROM wl_analysis')[0]['payload'],self.item['original_payload'])
        with patch.dict(os.environ,NOTION_TOKEN='test',NOTION_DATA_SOURCE_ID='test'), \
             patch('publish.provision_page',return_value='page'), patch('publish.verify_artifacts'), \
             patch('publish.update_report_metadata') as metadata, patch('publish.publish_report_blocks') as deliver:
            publish(self.store,self.run)
            self.assertIn(self.item['source_url'],json.dumps(deliver.call_args.args[3]))
            self.assertEqual(metadata.call_args.args[2],1)
            self.assertEqual(self.store.sql('SELECT evidence_key FROM wl_history'),[{'evidence_key':self.key}])

    def test_changed_evidence_and_frozen_run_refuse_new_writes(self):
        with self.assertRaises(ValueError):
            apply_correction(self.store,self.run,{**self.item,'source_title':'Another paper'})
        self.store.sql("UPDATE wl_runs SET status='PUBLISHING'")
        with self.assertRaises(ValueError):
            apply_correction(self.store,self.run,self.item)
        self.assertEqual(self.store.sql('SELECT * FROM wl_review_corrections'),[])
