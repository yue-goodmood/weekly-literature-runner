import contextlib
import datetime as dt
import io
import json
import os
import unittest
from unittest.mock import patch
import public_runner as r


class PublicRunnerTest(unittest.TestCase):
    def output(self, worker):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = r.safe_main('scheduled', worker)
        return code, out.getvalue()

    def test_private_output_and_errors_never_escape(self):
        def worker(mode):
            print('PRIVATE_ABSTRACT bearer secret-token')
            raise ValueError('PRIVATE_NOTION_URL and secret-token')
        code, out = self.output(worker)
        self.assertEqual(code, 1)
        self.assertEqual(out, 'weekly-literature: STOPPED_PRIVATE_DETAILS_SUPPRESSED\n')

    def test_status_allowlist_rejects_arbitrary_payload(self):
        code, out = self.output(lambda mode: 'PRIVATE_SUMMARY')
        self.assertEqual(code, 1)
        self.assertNotIn('PRIVATE_SUMMARY', out)

    def test_disabled_without_secrets_never_connects(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(r, 'Store') as store:
            self.assertEqual(r.execute('scheduled'), 'DISABLED')
            store.assert_not_called()

    def test_missing_config_never_connects(self):
        with patch.dict(os.environ, {'ENABLE_PRODUCTION':'true'}, clear=True), patch.object(r, 'Store') as store:
            self.assertRaises(r.ConfigurationError, r.execute, 'scheduled')
            store.assert_not_called()

    def test_daily_quota_stop_is_explicit_without_retry(self):
        calls = []
        def worker(mode):
            calls.append(mode)
            raise RuntimeError('D1_DAILY_QUOTA_EXCEEDED')
        code, out = self.output(worker)
        self.assertEqual(calls, ['scheduled'])
        self.assertEqual(code, 1)
        self.assertIn('D1_DAILY_QUOTA_EXCEEDED', out)

    def test_retriage_precedes_new_etl_and_publication(self):
        class Store:
            def migrate(self): pass
            def sql(self, *args):
                return [{'run_key':'private-run','audit':json.dumps({'review_policy':'unlimited-v1'})}]
        env = {key:'private-value' for key in r.REQUIRED}
        env['ENABLE_PRODUCTION'] = 'true'
        with patch.dict(os.environ, env), patch.object(r,'retune') as retune, patch.object(r,'run_etl') as etl, patch.object(r,'publish') as publish:
            self.assertEqual(r.execute('scheduled', Store(), dt.date(2026,11,2)), 'RETRIAGED')
            retune.assert_called_once()
            etl.assert_not_called()
            publish.assert_not_called()

    def test_pending_analysis_never_publishes(self):
        class Store:
            def migrate(self): pass
            def sql(self, sql, *args):
                if 'SELECT run_key,audit' in sql: return []
                return [{'run_key':'private-run','audit':json.dumps({'review_policy':'clinical-signal-unlimited-v1','triage':{'policy':r.TRIAGE_POLICY}}), 'selected':2, 'analyzed':1}]
        env = {key:'private-value' for key in r.REQUIRED}
        env['ENABLE_PRODUCTION'] = 'true'
        with patch.dict(os.environ, env), patch.object(r,'publish') as publish:
            self.assertEqual(r.execute('publish', Store()), 'AWAITING_REVIEW')
            publish.assert_not_called()

