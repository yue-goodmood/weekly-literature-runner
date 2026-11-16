import tempfile
import unittest
from pathlib import Path

import active_d1
from pipeline import Store


class ActiveD1Tests(unittest.TestCase):
    def test_index_transfer_is_resumable_and_copies_no_analysis(self):
        with tempfile.TemporaryDirectory() as tmp:
            archive = Store(str(Path(tmp) / 'archive.db'))
            active = Store(str(Path(tmp) / 'active.db'))
            archive.migrate()
            active.migrate()
            archive.sql('INSERT INTO wl_history VALUES (?,?)', ('PMID:1','r1'))
            archive.sql('INSERT INTO wl_analysis VALUES (?,?,?)', ('r2','PMID:2','private'))
            self.assertFalse(active_d1.seed_index(archive, active, max_pages=1))
            self.assertTrue(active_d1.seed_index(archive, active))
            self.assertTrue(active_d1.seed_index(archive, active))
            self.assertEqual([r['evidence_key'] for r in active.sql(
                'SELECT evidence_key FROM wl_history ORDER BY evidence_key')], ['PMID:1','PMID:2'])
            self.assertFalse(active.sql('SELECT 1 FROM wl_analysis'))
            self.assertEqual(archive.sql('SELECT payload FROM wl_analysis')[0]['payload'], 'private')
