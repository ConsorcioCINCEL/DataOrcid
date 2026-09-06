"""Integration checks against an explicitly disposable PostgreSQL database.

Run migrations first, then set DATAORCID_TEST_POSTGRES_URI to a database whose
name starts with dataorcid_test_. These tests clear application tables there.
"""

from concurrent.futures import ThreadPoolExecutor
import os
from threading import Event
import unittest
from unittest.mock import patch

from sqlalchemy import make_url, text

from app import create_app, db
from app.models import (
    EmailOutbox, InstitutionRegistry, InstitutionResearcher, InstitutionSyncVersion,
    SyncJob, User, WorkCache,
)
from app.services.background_jobs import _claim_next_job
from app.services.cache_service import build_full_cache_for_ror
from app.services.canonical_work_service import rebuild_canonical_works
from app.services.email_outbox import deliver_next_email, queue_account_email
from app.services.institution_lock import institution_write_lock

URI = os.environ.get('DATAORCID_TEST_POSTGRES_URI')


@unittest.skipUnless(URI, 'Requires an explicitly disposable PostgreSQL database.')
class PostgreSQLReliabilityTest(unittest.TestCase):
    def setUp(self):
        url = make_url(URI)
        if not url.drivername.startswith('postgresql') or not url.database.startswith('dataorcid_test_'):
            raise RuntimeError('Refusing to run destructive fixtures outside a disposable test database.')
        self.app = create_app({'TESTING': True, 'SQLALCHEMY_DATABASE_URI': URI})
        self.context = self.app.app_context()
        self.context.push()
        self.assertEqual(url, db.engine.url)
        with db.engine.begin() as connection:
            for table in reversed(db.metadata.sorted_tables):
                connection.execute(table.delete())

    def tearDown(self):
        db.session.remove()
        db.engine.dispose()
        self.context.pop()

    def test_institutional_and_global_locks_work_across_connections(self):
        same = Event()
        other = Event()
        global_scope = Event()

        def writer(scope, acquired):
            with self.app.app_context(), institution_write_lock(scope):
                acquired.set()

        with ThreadPoolExecutor(max_workers=3) as executor:
            with institution_write_lock('01first'):
                future_same = executor.submit(writer, '01first', same)
                future_other = executor.submit(writer, '02other', other)
                self.assertTrue(other.wait(3))
                future_global = executor.submit(writer, None, global_scope)
                self.assertFalse(same.wait(0.2))
                self.assertFalse(global_scope.wait(0.2))
            for future in (future_same, future_other, future_global):
                future.result(timeout=5)
        self.assertTrue(same.is_set())
        self.assertTrue(global_scope.is_set())

    def test_readers_keep_previous_data_until_the_publication_commit(self):
        institution = InstitutionRegistry(ror_id='01first', name='Test University')
        db.session.add(institution)
        db.session.flush()
        db.session.add_all([
            InstitutionResearcher(institution_id=institution.id, orcid='0001', is_active=True),
            WorkCache(ror_id='01first', orcid='0001', put_code=1, title='Previous publication'),
        ])
        db.session.commit()
        rebuild_canonical_works('01first')
        institution_id = institution.id
        observations = []

        def failed_derivation(*args, **kwargs):
            with db.engine.connect() as reader:
                observations.append(reader.execute(text('SELECT title FROM work_cache')).scalars().all())
                observations.append(reader.execute(text('SELECT count(*) FROM work_record_link')).scalar())
            raise RuntimeError('Stop before promotion')

        with patch('app.services.cache_service.discover_researchers_for_ror', return_value=([{'orcid-id': '0001'}], institution_id)), patch('app.services.cache_service.get_all_profiles_concurrently', return_value={'0001': {'person': {}}}), patch('app.services.analytics_service.refresh_openalex_facts', side_effect=failed_derivation):
            with self.assertRaisesRegex(RuntimeError, 'Stop before promotion'):
                build_full_cache_for_ror('01first', 'https://example.test', {})
        self.assertEqual([['Previous publication'], 1], observations)
        self.assertEqual('Previous publication', WorkCache.query.one().title)
        self.assertEqual('failed', InstitutionSyncVersion.query.one().status)

    def test_oai_publication_batches_preserve_all_records_and_withdrawals(self):
        from app.models import OaiPmhInstitutionConfig, OaiPmhRecordVersion
        from app.services.oai_publication_service import published_record_query
        db.session.add(OaiPmhInstitutionConfig(ror_id='01first', repository_name='Test repository', publication_policy='all'))
        db.session.add_all([
            WorkCache(ror_id='01first', orcid='0001', put_code=index, title=f'Article {index}',
                      type='journal-article', visibility='public', doi=f'10.1000/batch-{index}')
            for index in range(501)
        ])
        db.session.commit()
        rebuild_canonical_works('01first')
        self.assertEqual(501, OaiPmhRecordVersion.query.count())
        self.assertEqual(1, len({event.datestamp for event in OaiPmhRecordVersion.query.all()}))
        WorkCache.query.delete()
        rebuild_canonical_works('01first')
        self.assertEqual(1002, OaiPmhRecordVersion.query.count())
        self.assertEqual(501, published_record_query('01first').filter(OaiPmhRecordVersion.is_deleted.is_(True)).count())

    def test_two_queue_workers_claim_distinct_jobs(self):
        db.session.add_all([SyncJob(id=str(index), name=f'test-{index}', job_type='generic', status='queued') for index in range(2)])
        db.session.commit()

        def claim(worker):
            with self.app.app_context():
                return _claim_next_job(worker)

        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(claim, 'first-worker')
            second = executor.submit(claim, 'second-worker')
            self.assertEqual({'0', '1'}, {first.result(timeout=5), second.result(timeout=5)})

    def test_only_one_email_worker_delivers_a_claimed_intent(self):
        user = User(username='test@example.test', locale='en')
        user.set_password('test-only-password')
        db.session.add(user)
        db.session.commit()
        with self.app.test_request_context(base_url='http://localhost'):
            queue_account_email(user)
            db.session.commit()
        sending = Event()
        release = Event()

        def smtp(**kwargs):
            sending.set()
            if not release.wait(5):
                raise RuntimeError('Timed out waiting for the concurrent claim check.')
            return True, None

        def deliver():
            with self.app.app_context():
                return deliver_next_email()

        with patch('app.services.email_outbox.send_email', side_effect=smtp) as sender, ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(deliver)
            try:
                self.assertTrue(sending.wait(4))
                second = executor.submit(deliver)
                self.assertIsNone(second.result(timeout=3))
            finally:
                release.set()
            self.assertIsNotNone(first.result(timeout=3))
            sender.assert_called_once()
        db.session.expire_all()
        self.assertEqual('sent', EmailOutbox.query.one().status)
