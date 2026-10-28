import json
import os
import unittest
from unittest.mock import patch
from contextlib import ExitStack
import pipeline
import partial_publish as p


class PartialTest(unittest.TestCase):
    def setUp(self):
        self.store = pipeline.Store(':memory:')
        self.store.migrate()
        self.addCleanup(self.store.db.close)
        self.run = 'weekly-literature:2026-10-19:Asia/Shanghai'
        self.store.sql('INSERT INTO wl_runs VALUES(?,?,?,?,?,?)',
                       (self.run, 'h', '2026-09-28', '2026-10-18', 'READY', '{}'))
        self.calls = []
        self.fail = False
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(os.environ, {'NOTION_TOKEN':'test'}))
        self.stack.enter_context(patch.object(p, 'provision_page', side_effect=lambda s,r: 'page-'+r))
        self.stack.enter_context(patch.object(p, 'finalize_article_title'))
        self.stack.enter_context(patch.object(p, 'publish_draft'))
        self.stack.enter_context(patch.object(p, 'publish_report_blocks', side_effect=self.write))

    def write(self, store, artifact, page, blocks, token, **kwargs):
        self.calls.append((artifact, blocks, kwargs))
        if self.fail:
            raise TimeoutError('ambiguous append')

    def add(self, key, decision='include', analyzed=True):
        self.store.sql('INSERT INTO wl_articles VALUES(?,?)', (key,json.dumps({'title':'CRC study'})))
        self.store.sql('INSERT INTO wl_candidates VALUES(?,?,50,1)', (self.run,key))
        if analyzed:
            self.store.sql('INSERT INTO wl_analysis VALUES(?,?,?)',
                           (self.run,key,json.dumps({'decision':decision,'summary':'结果','popscience':'边界'})))

    def test_only_saved_decisions_delivered_once_and_no_final_state(self):
        self.add('PMID:1'); self.add('PMID:2','exclude'); self.add('PMID:3',analyzed=False)
        self.assertTrue(p.sync_partial(self.store,self.run,{'selected':3,'analyzed':2}))
        self.assertFalse(p.sync_partial(self.store,self.run,{'selected':3,'analyzed':2}))
        self.assertEqual(len(self.calls),2)
        text=json.dumps(self.calls[0][1],ensure_ascii=False)
        self.assertNotIn('正式归档',text)
        self.assertIn('不是最终周报',text)
        self.assertEqual(self.store.sql('SELECT * FROM wl_history'),[])
        self.assertEqual(self.store.sql('SELECT status FROM wl_runs')[0]['status'],'READY')

    def test_partial_batch_frozen_across_failure_and_new_analysis(self):
        self.add('PMID:2'); self.fail=True
        with self.assertRaises(TimeoutError):
            p.sync_partial(self.store,self.run,{'selected':2,'analyzed':1})
        self.add('PMID:1'); self.fail=False
        p.sync_partial(self.store,self.run,{'selected':2,'analyzed':2})
        index=p.cached(self.store,self.run,p.INDEX,[])
        self.assertEqual(index[0]['count'],1)
        self.assertEqual(self.store.sql('SELECT unit FROM wl_checkpoints WHERE unit LIKE ?',
                          (p.SHOWN+'%',))[0]['unit'],p.SHOWN+'PMID:2')
        p.sync_partial(self.store,self.run,{'selected':2,'analyzed':2})
        self.assertEqual(len(p.cached(self.store,self.run,p.INDEX,[])),2)

    def test_bounded_batches_append_index_without_resending_previous_rows(self):
        for n in range(51): self.add('PMID:'+str(n))
        p.sync_partial(self.store,self.run,{'selected':52,'analyzed':51})
        p.sync_partial(self.store,self.run,{'selected':52,'analyzed':51})
        entries=p.cached(self.store,self.run,p.INDEX,[])
        self.assertEqual([e['count'] for e in entries],[50,1])
        self.assertEqual(len(self.calls[-1][1]),1)
        self.assertEqual(len(self.calls[-1][2]['prefix']),3)

    def test_draft_failure_does_not_mark_batch_delivered(self):
        self.add('PMID:1')
        p.publish_draft.side_effect=ValueError('missing high article')
        with self.assertRaises(ValueError):
            p.sync_partial(self.store,self.run,{'selected':2,'analyzed':1})
        self.assertEqual(p.cached(self.store,self.run,p.INDEX,[]),[])
        self.assertIsNotNone(p.cached(self.store,self.run,p.ACTIVE,None))

    def test_completion_requires_independently_finalized_run(self):
        self.add('PMID:1')
        p.sync_partial(self.store,self.run,{'selected':2,'analyzed':1})
        with self.assertRaisesRegex(ValueError,'not published'):
            p.finish_progress(self.store,self.run)
        self.store.sql("UPDATE wl_runs SET status='PUBLISHED'")
        self.store.sql('INSERT INTO wl_publications(run_key,page_id) VALUES(?,?)',(self.run,'final'))
        p.finish_progress(self.store,self.run)
        self.assertTrue(p.cached(self.store,self.run,'notion:partial:finished:v1',False))

    def test_final_report_links_batches_without_copying_analysis_bodies(self):
        self.add('PMID:1')
        self.assertTrue(p.has_unsynced(self.store,self.run))
        p.sync_partial(self.store,self.run,{'selected':1,'analyzed':1})
        self.assertFalse(p.has_unsynced(self.store,self.run))
        state=self.store.sql('SELECT * FROM wl_runs')[0]
        rows=[{'payload':json.dumps({'decision':'include','summary':'DO_NOT_REPEAT_ANALYSIS'})}]
        blocks,count=p.final_index(state,rows,p.cached(self.store,self.run,p.INDEX,[]))
        self.assertEqual(count,1)
        self.assertNotIn('DO_NOT_REPEAT_ANALYSIS',json.dumps(blocks))
        self.assertIn('notion.so',json.dumps(blocks))

    def test_receipt_join_is_indexed(self):
        plan=self.store.sql('EXPLAIN QUERY PLAN SELECT c.evidence_key FROM wl_candidates c '
             'JOIN wl_analysis n ON n.run_key=c.run_key AND n.evidence_key=c.evidence_key '
             'LEFT JOIN wl_checkpoints p ON p.run_key=c.run_key AND p.unit=?||c.evidence_key '
             'WHERE c.run_key=? AND c.selected=1 AND p.unit IS NULL ORDER BY c.evidence_key LIMIT 50',
             (p.SHOWN,self.run))
        text=' '.join(row['detail'] for row in plan)
        self.assertNotIn('SCAN p',text)
        self.assertNotIn('SCAN n',text)
