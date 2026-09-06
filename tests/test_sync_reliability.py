"""Regression coverage for atomic publication, safe grouping, and durable mail."""

from datetime import timedelta
from pathlib import Path
import unittest
from unittest.mock import patch

from flask import Flask

from app import babel, db
from app.models import (
    CanonicalWorkOverride, EmailOutbox, InstitutionRegistry, InstitutionResearcher,
    InstitutionSyncProfile, InstitutionSyncVersion, User, WorkCache, WorkRecordLink, utc_now,
)
from app.services.cache_service import build_full_cache_for_ror, retry_failed_profiles_for_ror
from app.services.canonical_work_service import rebuild_canonical_works, review_canonical_records
from app.services.email_outbox import deliver_next_email, queue_account_email


class SyncReliabilityTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'app/templates'))
        self.app.config.update(SECRET_KEY='test', SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(self.app)
        babel.init_app(self.app)
        self.app.add_url_rule('/reset/<token>', endpoint='auth.reset_password', view_func=lambda token: token)
        self.app.add_url_rule('/manuals/<language>.pdf', endpoint='main.user_manual', view_func=lambda language: language)
        self.context = self.app.app_context()
        self.context.push()
        db.create_all()
        self.institution = InstitutionRegistry(ror_id='01test', name='Test University')
        db.session.add(self.institution)
        db.session.flush()
        db.session.add_all([
            InstitutionResearcher(institution_id=self.institution.id, orcid='0001', is_active=True, profile_status='success', profile_updated_at=utc_now()),
            InstitutionResearcher(institution_id=self.institution.id, orcid='0002', is_active=True, profile_status='success', profile_updated_at=utc_now()),
            WorkCache(ror_id='01test', orcid='0001', put_code=1, title='Previous first work'),
            WorkCache(ror_id='01test', orcid='0002', put_code=2, title='Previous second work'),
        ])
        db.session.commit()
        rebuild_canonical_works('01test')
        self.researchers = [{'orcid-id': '0001'}, {'orcid-id': '0002'}]
        self.discovery = patch('app.services.cache_service.discover_researchers_for_ror', return_value=(self.researchers, self.institution.id))
        self.discovery.start()

    def tearDown(self):
        self.discovery.stop()
        db.session.remove()
        db.drop_all()
        db.engine.dispose()
        self.context.pop()

    def test_partial_publication_retains_failed_data_and_retry_fetches_only_failures(self):
        with patch('app.services.cache_service.get_all_profiles_concurrently', return_value={'0001': {'person': {}, 'activities-summary': {}}}):
            result = build_full_cache_for_ror('01test', 'https://example.test', {})
        self.assertEqual(['0002'], result['failed_profiles'])
        self.assertEqual(1, result['retained_profiles'])
        self.assertEqual(2, result['requested_profiles'])
        self.assertTrue(result['errors'])
        self.assertEqual('partial', InstitutionSyncVersion.query.one().status)
        self.assertEqual('Previous second work', WorkCache.query.one().title)
        self.assertEqual(0, InstitutionSyncProfile.query.count())
        with patch('app.services.cache_service.get_all_profiles_concurrently', return_value={'0002': {'person': {}, 'activities-summary': {}}}) as fetch:
            result = retry_failed_profiles_for_ror('01test', 'https://example.test', {})
        fetch.assert_called_once_with(['0002'], max_workers=10)
        self.assertFalse(result['errors'])
        self.assertEqual(0, InstitutionResearcher.query.filter_by(profile_status='failed').count())
        self.assertEqual(2, InstitutionResearcher.query.filter_by(is_active=True).count())

    def test_derived_failure_keeps_the_entire_previous_publication(self):
        before = [(row.id, row.title) for row in WorkCache.query.order_by(WorkCache.id)]
        links = [(row.work_cache_id, row.canonical_work_id) for row in WorkRecordLink.query.order_by(WorkRecordLink.id)]
        with patch('app.services.cache_service.get_all_profiles_concurrently', return_value={'0001': {'person': {}}, '0002': {'person': {}}}), patch('app.services.analytics_service.refresh_openalex_facts', side_effect=RuntimeError('facts failed')):
            with self.assertRaisesRegex(RuntimeError, 'facts failed'):
                build_full_cache_for_ror('01test', 'https://example.test', {})
        self.assertEqual(before, [(row.id, row.title) for row in WorkCache.query.order_by(WorkCache.id)])
        self.assertEqual(links, [(row.work_cache_id, row.canonical_work_id) for row in WorkRecordLink.query.order_by(WorkRecordLink.id)])
        self.assertEqual('failed', InstitutionSyncVersion.query.one().status)
        self.assertEqual(2, InstitutionResearcher.query.filter_by(profile_status='success').count())
        self.assertEqual(0, InstitutionSyncProfile.query.count())

    def test_remote_fetch_runs_before_any_published_rows_are_replaced(self):
        def fetch(batch, max_workers):
            self.assertEqual(2, WorkCache.query.count())
            self.assertEqual(2, WorkRecordLink.query.count())
            self.assertEqual('preparing', InstitutionSyncVersion.query.one().status)
            return {orcid: {'person': {}} for orcid in batch}
        with patch('app.services.cache_service.get_all_profiles_concurrently', side_effect=fetch):
            build_full_cache_for_ror('01test', 'https://example.test', {})
        self.assertEqual('published', InstitutionSyncVersion.query.one().status)
        self.assertEqual(0, WorkCache.query.count())
        self.assertEqual(0, WorkRecordLink.query.count())

    def test_title_matches_require_review_and_split_decisions_survive_rebuild(self):
        for row in WorkCache.query.all():
            row.title = 'Editorial'
            row.pub_year = '2025'
        db.session.commit()
        self.assertEqual(2, rebuild_canonical_works('01test')['unique_outputs'])
        ids = [row.id for row in WorkCache.query.all()]
        review_canonical_records('01test', ids, merge=True, reason='Confirmed same work against the journal.', user_id=1)
        self.assertEqual(1, rebuild_canonical_works('01test')['unique_outputs'])
        review_canonical_records('01test', ids, merge=False, reason='Different editorial authors.', user_id=1)
        self.assertEqual(2, rebuild_canonical_works('01test')['unique_outputs'])
        self.assertEqual(2, CanonicalWorkOverride.query.count())
        self.assertTrue(all(row.reason == 'Different editorial authors.' for row in CanonicalWorkOverride.query.all()))

    def test_name_refresh_updates_published_oai_creators(self):
        from app.models import OaiPmhInstitutionConfig, OaiPmhRecordVersion, ResearcherCache
        from app.services.cache_service import build_researcher_names_cache
        from app.services.oai_publication_service import published_record_query
        work = WorkCache.query.filter_by(orcid='0001').one()
        work.visibility = 'public'
        db.session.add(ResearcherCache(orcid='0001', credit_name='Previous name'))
        db.session.add(OaiPmhInstitutionConfig(ror_id='01test', repository_name='Test repository', publication_policy='all'))
        db.session.commit()
        rebuild_canonical_works('01test')
        with patch('app.services.cache_service.get_all_profiles_concurrently', return_value={
            '0001': {'person': {'name': {'credit-name': {'value': 'Updated name'}}}},
            '0002': {'person': {}},
        }):
            build_researcher_names_cache('01test')
        record = published_record_query('01test').filter(OaiPmhRecordVersion.is_deleted.is_(False)).one()
        self.assertEqual(['Updated name'], record.payload_json['creators'])

    def _user(self):
        user = User(username='person@example.test', locale='en')
        user.set_password('unchanged-password')
        db.session.add(user)
        db.session.commit()
        return user

    def test_outbox_is_rolled_back_with_the_account_transaction(self):
        user = self._user()
        with self.app.test_request_context():
            queue_account_email(user)
            db.session.flush()
            db.session.rollback()
        self.assertEqual(0, EmailOutbox.query.count())

    def test_outbox_retries_without_changing_password_or_sending_passwords(self):
        user = self._user()
        before = user.password_hash
        with self.app.test_request_context():
            first = queue_account_email(user)
            self.assertEqual(first, queue_account_email(user))
            db.session.commit()
        with patch('app.services.email_outbox.send_email', return_value=(False, 'SMTP unavailable')):
            deliver_next_email()
        item = EmailOutbox.query.one()
        self.assertEqual('pending', item.status)
        self.assertEqual(before, user.password_hash)
        item.available_at = utc_now() - timedelta(seconds=1)
        db.session.commit()
        with patch('app.services.email_outbox.send_email', return_value=(True, None)) as send:
            deliver_next_email()
        self.assertEqual('sent', item.status)
        self.assertIn('/reset/', send.call_args.kwargs['text'])
        self.assertIn('/manuals/en.pdf', send.call_args.kwargs['text'])
        self.assertNotIn('unchanged-password', send.call_args.kwargs['text'])
        self.assertNotIn('Temporary Password', send.call_args.kwargs['text'])
        self.assertEqual(before, user.password_hash)

    def test_repeated_worker_interruptions_reach_the_email_retry_limit(self):
        user = self._user()
        with self.app.test_request_context():
            queue_account_email(user)
            db.session.commit()
        item = EmailOutbox.query.one()
        item.status = 'sending'
        item.attempts = 5
        item.claimed_at = utc_now() - timedelta(minutes=10)
        db.session.commit()
        with patch('app.services.email_outbox.send_email') as send:
            deliver_next_email()
        send.assert_not_called()
        self.assertEqual('failed', item.status)
        self.assertTrue(user.check_password('unchanged-password'))

    def test_password_change_cancels_an_obsolete_pending_access_email(self):
        user = self._user()
        with self.app.test_request_context():
            queue_account_email(user)
            db.session.commit()
        user.set_password('replacement-password')
        db.session.commit()
        with patch('app.services.email_outbox.send_email') as send:
            deliver_next_email()
        send.assert_not_called()
        self.assertEqual('cancelled', EmailOutbox.query.one().status)
