"""Add atomic synchronization versions, canonical decisions, OAI history and email outbox

Revision ID: 53e7ddc83bf3
Revises: a1b3c5d7e9f2
Create Date: 2026-09-06 13:25:19.634812

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '53e7ddc83bf3'
down_revision = 'a1b3c5d7e9f2'
branch_labels = None
depends_on = None


def upgrade():
    # Early create_all-based installations have unique indexes but lack these
    # baseline constraints. Fresh Alembic databases already contain both.
    inspector = sa.inspect(op.get_bind())
    if op.get_bind().dialect.name == 'postgresql':
        for table, column in (
            ('canonical_work', 'canonical_key'),
            ('duplicate_profile_review', 'group_key'),
        ):
            if not any(item['column_names'] == [column] for item in inspector.get_unique_constraints(table)):
                op.create_unique_constraint(f'{table}_{column}_key', table, [column])

    op.create_table('canonical_work_override',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('ror_id', sa.String(length=32), nullable=False),
    sa.Column('orcid', sa.String(length=32), nullable=False),
    sa.Column('source_record_key', sa.String(length=96), nullable=False),
    sa.Column('canonical_key', sa.String(length=80), nullable=False),
    sa.Column('reason', sa.Text(), nullable=False),
    sa.Column('updated_by_user_id', sa.Integer(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('ror_id', 'orcid', 'source_record_key')
    )
    with op.batch_alter_table('canonical_work_override', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_canonical_work_override_ror_id'), ['ror_id'], unique=False)

    op.create_table('institution_sync_version',
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('ror_id', sa.String(length=32), nullable=False),
    sa.Column('status', sa.String(length=16), nullable=False),
    sa.Column('researchers_json', sa.JSON(), nullable=False),
    sa.Column('result_json', sa.JSON(), nullable=True),
    sa.Column('error', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('published_at', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    with op.batch_alter_table('institution_sync_version', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_institution_sync_version_ror_id'), ['ror_id'], unique=False)

    op.create_table('oai_pmh_record_version',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('ror_id', sa.String(length=32), nullable=False),
    sa.Column('canonical_key', sa.String(length=80), nullable=False),
    sa.Column('datestamp', sa.DateTime(), nullable=False),
    sa.Column('is_deleted', sa.Boolean(), nullable=False),
    sa.Column('content_hash', sa.String(length=64), nullable=False),
    sa.Column('payload_json', sa.JSON(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    with op.batch_alter_table('oai_pmh_record_version', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_oai_pmh_record_version_datestamp'), ['datestamp'], unique=False)
        batch_op.create_index(batch_op.f('ix_oai_pmh_record_version_ror_id'), ['ror_id'], unique=False)
        batch_op.create_index('ix_oai_record_ror_key_revision', ['ror_id', 'canonical_key', 'id'], unique=False)

    op.create_table('email_outbox',
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('kind', sa.String(length=24), nullable=False),
    sa.Column('base_url', sa.Text(), nullable=False),
    sa.Column('password_fingerprint', sa.String(length=64), nullable=False),
    sa.Column('status', sa.String(length=16), nullable=False),
    sa.Column('attempts', sa.Integer(), nullable=False),
    sa.Column('available_at', sa.DateTime(), nullable=False),
    sa.Column('claimed_at', sa.DateTime(), nullable=True),
    sa.Column('claim_token', sa.String(length=36), nullable=True),
    sa.Column('error', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('sent_at', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['user_id'], ['user.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    with op.batch_alter_table('email_outbox', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_email_outbox_status'), ['status'], unique=False)
        batch_op.create_index(batch_op.f('ix_email_outbox_user_id'), ['user_id'], unique=False)

    op.create_table('institution_sync_profile',
    sa.Column('version_id', sa.String(length=36), nullable=False),
    sa.Column('orcid', sa.String(length=32), nullable=False),
    sa.Column('payload_json', sa.JSON(), nullable=True),
    sa.ForeignKeyConstraint(['version_id'], ['institution_sync_version.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('version_id', 'orcid')
    )
    with op.batch_alter_table('oai_pmh_institution_config', schema=None) as batch_op:
        batch_op.add_column(sa.Column('publication_initialized_at', sa.DateTime(), nullable=True))



def downgrade():
    # Keep repaired baseline constraints: they belong to earlier migrations.
    with op.batch_alter_table('oai_pmh_institution_config', schema=None) as batch_op:
        batch_op.drop_column('publication_initialized_at')

    op.drop_table('institution_sync_profile')
    with op.batch_alter_table('email_outbox', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_email_outbox_user_id'))
        batch_op.drop_index(batch_op.f('ix_email_outbox_status'))

    op.drop_table('email_outbox')
    with op.batch_alter_table('oai_pmh_record_version', schema=None) as batch_op:
        batch_op.drop_index('ix_oai_record_ror_key_revision')
        batch_op.drop_index(batch_op.f('ix_oai_pmh_record_version_ror_id'))
        batch_op.drop_index(batch_op.f('ix_oai_pmh_record_version_datestamp'))

    op.drop_table('oai_pmh_record_version')
    with op.batch_alter_table('institution_sync_version', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_institution_sync_version_ror_id'))

    op.drop_table('institution_sync_version')
    with op.batch_alter_table('canonical_work_override', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_canonical_work_override_ror_id'))

    op.drop_table('canonical_work_override')
