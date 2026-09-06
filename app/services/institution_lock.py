"""Coordinate institutional writers across web workers, CLI jobs, and commits."""

from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from threading import RLock

from sqlalchemy import text

from .. import db

_held_scope = ContextVar("institution_write_scope", default=())
_local_lock = RLock()


@contextmanager
def institution_write_lock(ror_id=None):
    """Hold a session-level PostgreSQL lock until the complete pipeline finishes.

    Global writers exclude all institutional writers. Nested services reuse their
    caller's lock; the dedicated connection keeps it alive across ORM commits.
    """
    held = _held_scope.get()
    if held and (held[0] is None or ror_id in held):
        yield
        return
    if held:
        raise RuntimeError("Cannot expand an institutional write lock inside a transaction.")
    token = _held_scope.set((ror_id,))
    try:
        if db.engine.dialect.name != "postgresql":
            with _local_lock:
                yield
            return
        with db.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            shared = "_shared" if ror_id else ""
            connection.execute(text(f"SELECT pg_advisory_lock{shared}(hashtextextended('dataorcid:writers', 2))"))
            try:
                if ror_id:
                    connection.execute(text("SELECT pg_advisory_lock(hashtextextended(:scope, 2))"), {"scope": f"dataorcid:institution:{ror_id}"})
                try:
                    yield
                finally:
                    if ror_id:
                        connection.execute(text("SELECT pg_advisory_unlock(hashtextextended(:scope, 2))"), {"scope": f"dataorcid:institution:{ror_id}"})
            finally:
                connection.execute(text(f"SELECT pg_advisory_unlock{shared}(hashtextextended('dataorcid:writers', 2))"))
    finally:
        _held_scope.reset(token)


def institutional_writer(function):
    """Decorate services whose first argument is their institution ROR scope."""
    @wraps(function)
    def wrapped(*args, **kwargs):
        ror_id = args[0] if args else kwargs.get("ror_id")
        with institution_write_lock(ror_id):
            return function(*args, **kwargs)
    return wrapped


def institutional_request_writer(function):
    """Serialize OAI mutations after the existing institution/role decorators."""
    @wraps(function)
    def wrapped(*args, **kwargs):
        from flask import g
        from ..models import OaiPmhInstitutionConfig
        from .oai_publication_service import ensure_oai_publication
        with institution_write_lock(g.institution_ror_id):
            config = OaiPmhInstitutionConfig.query.filter_by(ror_id=g.institution_ror_id).first()
            if config:
                ensure_oai_publication(config)
            return function(*args, **kwargs)
    return wrapped
