import contextlib
import datetime as dt
import io
import json
import os
import tempfile
import unittest
import urllib.error
from unittest.mock import patch
import public_runner as r


class PublicRunnerTest(unittest.TestCase):
    def setUp(self):
        # Existing phase tests isolate orchestration; ledger integration below
        # exercises real SQLite receipts, crashes, UTC rollover and exhaustion.
        reservation=patch.object(r,'reserve_retrieval',return_value=True)
        reservation.start()
        self.addCleanup(reservation.stop)

    def test_database_pause_prints_only_allowlisted_resource_counters(self):
        def worker(mode):
            raise r.PhaseError('BACKFILL',r.DatabaseBudgetReached({
                'reads':250012,'writes':100,'read_limit':250000,'write_limit':10000,
                'private_query':'DO_NOT_PRINT'}))
        code,out=self.output(worker)
        self.assertEqual(code,0)
        self.assertIn('reads=250012 writes=100 read_limit=250000 write_limit=10000',out)
        self.assertNotIn('DO_NOT_PRINT',out)

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


class RetrievalRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=self.tmp.name+'/ledger.db'
        self.store=r.Store(self.path);self.addCleanup(self.store.db.close)
        self.store.migrate()
        self.now=dt.datetime(2026,11,2,23,59,tzinfo=dt.timezone.utc)
        env={key:'test' for key in r.REQUIRED};env['ENABLE_PRODUCTION']='true'
        change=patch.dict(os.environ,env);change.start();self.addCleanup(change.stop)

    def fresh_store(self):
        store=r.Store(self.path);self.addCleanup(store.db.close)
        return store

    def test_abandoned_leases_survive_restart_and_remain_charged(self):
        for _ in range(4): self.assertTrue(r.reserve_retrieval(self.fresh_store(),self.now))
        self.assertFalse(r.reserve_retrieval(self.fresh_store(),self.now))

    def test_utc_reset_and_environment_cannot_raise_budgets(self):
        self.store.row_read_budget=9999999;self.store.row_write_budget=9999999
        self.assertTrue(r.reserve_retrieval(self.store,self.now))
        self.assertEqual((self.store.row_read_budget,self.store.row_write_budget),(249900,9900))
        self.assertEqual(self.store.accounting_limits,(250000,10000))
        for _ in range(3): self.assertTrue(r.reserve_retrieval(self.fresh_store(),self.now))
        self.assertFalse(r.reserve_retrieval(self.fresh_store(),self.now))
        self.assertTrue(r.reserve_retrieval(self.fresh_store(),self.now+dt.timedelta(minutes=1)))

    def test_more_than_three_cheap_attempts_charge_measured_usage(self):
        for _ in range(8):
            store=self.fresh_store()
            self.assertTrue(r.reserve_retrieval(store,self.now))
            store.rows_read=100;store.rows_written=50
            r.settle_retrieval(store)
            r.settle_retrieval(store)  # identical finalization cannot double charge
        rows=self.store.sql("SELECT payload FROM wl_checkpoints WHERE run_key=?",('system:retrieval-budget:2026-11-02',))
        self.assertEqual(sum(json.loads(row['payload'])['charged_writes'] for row in rows),480)
        self.assertTrue(all(json.loads(row['payload'])['state']=='SETTLED' for row in rows))

    def test_old_slots_are_conservatively_counted_not_deleted_or_reset(self):
        key='system:retrieval-budget:2026-11-02'
        for i in range(3):
            self.store.sql('INSERT INTO wl_checkpoints VALUES(?,?,?)',(key,'slot:'+str(i),json.dumps({
                'read_budget':250000,'write_budget':10000,'write_headroom':5000})))
        self.assertTrue(r.reserve_retrieval(self.fresh_store(),self.now))
        self.assertFalse(r.reserve_retrieval(self.fresh_store(),self.now))
        self.assertEqual(self.store.sql("SELECT count(*) AS n FROM wl_checkpoints WHERE run_key=? AND unit LIKE 'slot:%'",(key,))[0]['n'],3)

    def test_settlement_has_reserved_room_after_work_pause_but_not_full_exhaustion(self):
        store=self.fresh_store();r.reserve_retrieval(store,self.now)
        store.rows_written=9950
        r.settle_retrieval(store)
        payload=json.loads(store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=?',('system:retrieval-budget:2026-11-02',))[0]['payload'])
        self.assertEqual(payload['charged_writes'],9960)
        store=self.fresh_store();r.reserve_retrieval(store,self.now)
        store.rows_written=10001
        with patch.object(store,'sql',wraps=store.sql) as sql: r.settle_retrieval(store)
        sql.assert_not_called()

    def test_quota_exhaustion_never_attempts_settlement(self):
        store=self.fresh_store();r.reserve_retrieval(store,self.now);store.quota_exhausted=True
        with patch.object(store,'sql') as sql: r.settle_retrieval(store)
        sql.assert_not_called()

    def test_missing_usage_metadata_never_refunds_reservation(self):
        store=self.fresh_store();r.reserve_retrieval(store,self.now);store.usage_complete=False
        with patch.object(store,'sql') as sql: r.settle_retrieval(store)
        sql.assert_not_called()

    def test_failed_settlement_leaves_reservation_fully_charged(self):
        store=self.fresh_store();r.reserve_retrieval(store,self.now)
        with patch.object(store,'sql',side_effect=TimeoutError()):
            with self.assertRaises(TimeoutError): r.settle_retrieval(store)
        payload=json.loads(self.store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=?',('system:retrieval-budget:2026-11-02',))[0]['payload'])
        self.assertEqual(payload['state'],'RESERVED');self.assertEqual(payload['charged_writes'],15000)

    def test_remaining_balance_can_grant_smaller_allowance(self):
        key='system:retrieval-budget:2026-11-02'
        self.store.sql('INSERT INTO wl_checkpoints VALUES(?,?,?)',(key,'usage:prior',json.dumps({
            'state':'SETTLED','charged_reads':0,'charged_writes':57000})))
        store=self.fresh_store();self.assertTrue(r.reserve_retrieval(store,self.now))
        self.assertEqual(store.row_write_budget,1900)
        self.assertFalse(r.reserve_retrieval(self.fresh_store(),self.now))

    def count_slots(self):
        return self.store.sql("SELECT count(*) AS n FROM wl_checkpoints WHERE run_key LIKE 'system:retrieval-budget:%'")[0]['n']

    def seed(self):
        self.store.sql('INSERT INTO wl_runs VALUES(?,?,?,?,?,?)',('synthetic','hash','2026-09-28','2026-10-18','READY',json.dumps({
            'review_policy':'clinical-signal-unlimited-v1','triage':{'policy':r.TRIAGE_POLICY},
            'scope':{'policy':r.TRIAGE_POLICY,'coverage':'legacy_queue_retriage_only'}})))

    def test_idle_recovery_never_creates_new_week_or_consumes_slot(self):
        with patch.object(r,'run_etl') as etl:
            self.assertEqual(r.execute('recovery',self.store),'NO_PENDING_PUBLICATION')
        etl.assert_not_called();self.assertEqual(self.count_slots(),0)

    def test_daily_exhaustion_prevents_retrieval(self):
        self.seed()
        for _ in range(4): self.assertTrue(r.reserve_retrieval(self.fresh_store()))
        with patch('backfill.resume') as resume,patch.object(r,'run_etl') as etl:
            self.assertEqual(r.execute('recovery',self.store),'PAUSED_RETRIEVAL_DAILY_BUDGET')
        resume.assert_not_called();etl.assert_not_called()

    def test_completion_checks_publication_with_same_store_and_one_slot(self):
        self.seed();calls=[]
        def complete(store,run,*args):
            calls.append(store)
            audit={'review_policy':'clinical-signal-unlimited-v1','triage':{'policy':r.TRIAGE_POLICY}}
            store.sql('UPDATE wl_runs SET audit=? WHERE run_key=?',(json.dumps(audit),run))
        with patch('backfill.resume',side_effect=complete),patch.object(r,'sync_partial',return_value=False), \
             patch.object(r,'has_unsynced',return_value=False),patch.object(r,'publish') as publish,patch.object(r,'finish_progress'):
            self.assertEqual(r.execute('recovery',self.store),'PUBLICATION_VERIFIED')
        self.assertEqual(calls,[self.store]);publish.assert_called_once_with(self.store,'synthetic')
        self.assertEqual(self.count_slots(),1)

    def test_budget_pause_preserves_slot_and_does_not_retry(self):
        self.seed()
        with patch('backfill.resume',side_effect=r.DatabaseBudgetReached()) as resume:
            with contextlib.redirect_stdout(io.StringIO()) as out:
                code=r.safe_main('recovery',lambda mode:r.execute(mode,self.store))
        self.assertEqual(code,0);self.assertIn('PAUSED_D1_RUN_BUDGET',out.getvalue())
        resume.assert_called_once();self.assertEqual(self.count_slots(),1)

    def test_daily_quota_immediately_stops_without_publication(self):
        self.seed()
        with patch('backfill.resume',side_effect=RuntimeError('D1_DAILY_QUOTA_EXCEEDED')) as resume,patch.object(r,'publish') as publish:
            with self.assertRaises(r.PhaseError): r.execute('recovery',self.store)
        resume.assert_called_once();publish.assert_not_called()

    def test_workflow_routes_and_serialization(self):
        workflow=(r.ROOT.parent/'.github/workflows/literature.yml').read_text()
        self.assertIn("cron: '15 * * * *'",workflow)
        self.assertIn("github.event_name == 'push' && 'recovery'",workflow)
        self.assertIn('cancel-in-progress: false',workflow)
        self.assertNotIn('actions: write',workflow)



