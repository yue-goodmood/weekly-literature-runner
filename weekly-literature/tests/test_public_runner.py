import contextlib
import datetime as dt
import io
import json
import os
import unittest
import urllib.error
from unittest.mock import patch
import public_runner as r


class PublicRunnerTest(unittest.TestCase):
    def test_publication_never_restarts_pending_backfill_or_finalizes(self):
        audit={'review_policy':'clinical-signal-unlimited-v1','triage':{'policy':r.TRIAGE_POLICY},
               'scope':{'policy':r.TRIAGE_POLICY,'coverage':'legacy_queue_retriage_only'}}
        class Store:
            def migrate(self): pass
            def sql(self,sql,*args):
                if sql.startswith('SELECT payload'): return []
                return [{'run_key':'synthetic-run','status':'READY','audit':json.dumps(audit),
                         'selected':10,'analyzed':10}]
        order=[];env={k:'test' for k in r.REQUIRED};env['ENABLE_PRODUCTION']='true'
        with patch.dict(os.environ,env),patch.object(r,'sync_partial',side_effect=lambda *a:order.append('deliver') or False), \
             patch('backfill.resume') as backfill,patch.object(r,'publish') as final:
            self.assertEqual(r.execute('publish',Store()),'AWAITING_BACKFILL')
        self.assertEqual(order,['deliver']);final.assert_not_called();backfill.assert_not_called()

    def test_snapshot_pause_is_not_completion_and_exposes_no_payload(self):
        from supplements import SourceSnapshotChanged
        def worker(mode): raise r.PhaseError('BACKFILL',SourceSnapshotChanged('PRIVATE_QUERY'))
        code,out=self.output(worker)
        self.assertEqual(code,0)
        self.assertEqual(out,'weekly-literature: PAUSED_SOURCE_SNAPSHOT_CHANGED\n')

    def test_complete_analysis_waits_for_all_notion_batches(self):
        class Store:
            def migrate(self): pass
            def sql(self,*args):
                if 'SELECT payload' in args[0]: return []
                return [{'run_key':'private-run','status':'READY','audit':json.dumps({
                    'review_policy':'clinical-signal-unlimited-v1','triage':{'policy':r.TRIAGE_POLICY}}),
                    'selected':1000,'analyzed':1000}]
        env={key:'test' for key in r.REQUIRED}; env['ENABLE_PRODUCTION']='true'
        with patch.dict(os.environ,env), patch.object(r,'sync_partial',side_effect=[True,False]), \
             patch.object(r,'has_unsynced',return_value=True), patch.object(r,'publish') as publish:
            self.assertEqual(r.execute('publish',Store()),'PARTIAL_SYNC_VERIFIED')
            publish.assert_not_called()

    def test_final_index_repair_after_published_run_failure(self):
        class Store:
            def migrate(self): pass
            def sql(self,*args):
                return [{'run_key':'private-run','status':'PUBLISHED','audit':json.dumps({
                    'review_policy':'clinical-signal-unlimited-v1','triage':{'policy':r.TRIAGE_POLICY}})}]
        env={key:'test' for key in r.REQUIRED}; env['ENABLE_PRODUCTION']='true'
        with patch.dict(os.environ,env), patch.object(r,'finish_progress') as finish, \
             patch.object(r,'sync_partial') as sync:
            self.assertEqual(r.execute('publish',Store()),'PUBLICATION_VERIFIED')
            finish.assert_called_once_with(unittest.mock.ANY,'private-run')
            sync.assert_not_called()

    def test_database_budget_pause_is_not_reported_as_daily_quota_or_completion(self):
        def worker(mode): raise r.PhaseError('ETL',r.DatabaseBudgetReached())
        code,out=self.output(worker)
        self.assertEqual(code,0)
        self.assertEqual(out,'weekly-literature: PAUSED_D1_RUN_BUDGET\n')

    def test_fixed_diagnostic_exposes_no_identifiers_or_upstream_details(self):
        def worker(mode):
            raise r.PhaseError('ETL',ValueError('Conflicting identifier aliases require reconciliation: PRIVATE_TOKEN'))
        code,out=self.output(worker)
        self.assertEqual(code,1)
        self.assertEqual(out,'weekly-literature: ETL_IDENTIFIER_CONFLICT\n')

    def test_completed_week_skips_retrieval(self):
        class Store:
            def migrate(self): pass
            def sql(self, sql, *args):
                if sql == 'SELECT status FROM wl_runs WHERE run_key=?':
                    return [{'status':'READY'}]
                return []
        env = {key:'test-value' for key in r.REQUIRED[:3]}
        env['ENABLE_PRODUCTION'] = 'true'
        with patch.dict(os.environ, env), patch.object(r, 'run_etl') as etl:
            self.assertEqual(r.execute('etl', Store(), dt.date(2026,10,28)), 'NO_PENDING_ETL')
            etl.assert_not_called()

    def test_completed_etl_does_not_skip_combined_publication_check(self):
        class Store:
            def migrate(self): pass
            def sql(self, sql, *args):
                if sql == 'SELECT status FROM wl_runs WHERE run_key=?':
                    return [{'status':'READY'}]
                return []
        env = {key:'test-value' for key in r.REQUIRED}
        env['ENABLE_PRODUCTION'] = 'true'
        with patch.dict(os.environ, env), patch.object(r, 'run_etl') as etl:
            self.assertEqual(r.execute('scheduled', Store()), 'NO_PENDING_PUBLICATION')
            etl.assert_not_called()

    def test_checkpointed_pauses_are_explicit_and_do_not_trigger_failure_retry(self):
        for exception, label in [(r.WorkBudgetReached(), 'PAUSED_WORK_BUDGET'),
                                 (r.UpstreamDeferred(), 'PAUSED_UPSTREAM_COOLDOWN')]:
            def worker(mode):
                raise r.PhaseError('ETL', exception)
            code, out = self.output(worker)
            self.assertEqual(code, 0)
            self.assertEqual(out, 'weekly-literature: '+label+'\n')

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
                if 'SELECT payload' in sql: return []
                if 'SELECT run_key,audit' in sql: return []
                return [{'run_key':'private-run','audit':json.dumps({'review_policy':'clinical-signal-unlimited-v1','triage':{'policy':r.TRIAGE_POLICY}}), 'selected':2, 'analyzed':1}]
        env = {key:'private-value' for key in r.REQUIRED}
        env['ENABLE_PRODUCTION'] = 'true'
        with patch.dict(os.environ, env), patch.object(r,'publish') as publish, patch.object(r,'sync_partial',side_effect=[True]*6+[False]) as sync:
            self.assertEqual(r.execute('publish', Store()), 'PARTIAL_SYNC_VERIFIED')
            self.assertEqual(sync.call_count,7)
            publish.assert_not_called()

    def test_delivery_checks_work_budget_before_starting_each_batch(self):
        class Store:
            def migrate(self): pass
            def sql(self,sql,*args):
                if 'SELECT payload' in sql or 'SELECT run_key,audit' in sql: return []
                return [{'run_key':'private-run','status':'READY','audit':json.dumps({
                    'review_policy':'clinical-signal-unlimited-v1','triage':{'policy':r.TRIAGE_POLICY}}),
                    'selected':1000,'analyzed':1000}]
        env={k:'test' for k in r.REQUIRED};env['ENABLE_PRODUCTION']='true'
        with patch.dict(os.environ,env),patch.object(r,'check_budget',side_effect=[None,r.WorkBudgetReached()]), \
             patch.object(r,'sync_partial',return_value=True) as sync:
            with self.assertRaises(r.WorkBudgetReached): r.execute('publish',Store())
            self.assertEqual(sync.call_count,1)

    def test_oldest_etl_resumes_with_its_original_frozen_config(self):
        frozen={'version':'old','queries':{}}
        class Store:
            def sql(self,sql,*args):
                if "status='ETL'" in sql:
                    return [{'run_key':'weekly-literature:2026-10-19:Asia/Shanghai','config_hash':r.digest(frozen)}]
                return [{'payload':json.dumps(frozen)}]
        date,config=r.etl_target(Store(),dt.date(2026,11,2),{'version':'new'})
        self.assertEqual(date,'2026-10-19')
        self.assertEqual(config,frozen)

    def test_legacy_unfrozen_config_mismatch_fails_closed(self):
        class Store:
            def sql(self,sql,*args):
                if "status='ETL'" in sql:
                    return [{'run_key':'weekly-literature:2026-10-19:Asia/Shanghai','config_hash':'unknown-original'}]
                return []
        self.assertRaises(r.FrozenConfigError,r.etl_target,Store(),dt.date(2026,11,2),{})

    def test_missing_monday_run_can_start_on_wednesday(self):
        class Store:
            def sql(self,*args): return []
        date,_=r.etl_target(Store(),dt.date(2026,10,28),{})
        self.assertEqual(date,'2026-10-26')

    def test_source_retrieval_does_not_require_notion_credentials(self):
        class Store:
            def migrate(self): pass
            def sql(self,*args): return []
        env={key:'test-value' for key in r.REQUIRED[:3]}
        env['ENABLE_PRODUCTION']='true'
        with patch.dict(os.environ,env,clear=True), patch.object(r,'run_etl') as etl:
            self.assertEqual(r.execute('etl',Store(),dt.date(2026,10,28)),'ETL_CHECKPOINTED')
            etl.assert_called_once()

    def test_publish_mode_never_retriages_or_runs_etl(self):
        class Store:
            def migrate(self): pass
            def sql(self,*args): return []
        env={key:'test-value' for key in r.REQUIRED}
        env['ENABLE_PRODUCTION']='true'
        with patch.dict(os.environ,env),patch.object(r,'retune') as retune,patch.object(r,'run_etl') as etl:
            self.assertEqual(r.execute('publish',Store()),'NO_PENDING_PUBLICATION')
            retune.assert_not_called()
            etl.assert_not_called()

    def test_private_phase_error_exposes_only_service_and_http_code(self):
        def worker(mode):
            raise r.PhaseError('NOTION',urllib.error.HTTPError('https://example.org/private',403,'PRIVATE',{},None))
        code,out=self.output(worker)
        self.assertEqual(code,1)
        self.assertEqual(out,'weekly-literature: NOTION_HTTP_403\n')

    def test_wrapped_daily_quota_keeps_safe_explicit_status(self):
        def worker(mode):
            raise r.PhaseError('ETL',RuntimeError('D1_DAILY_QUOTA_EXCEEDED'))
        code,out=self.output(worker)
        self.assertEqual(code,1)
        self.assertEqual(out,'weekly-literature: D1_DAILY_QUOTA_EXCEEDED\n')


