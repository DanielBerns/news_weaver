"""Database engine and session factory for the pipeline database.

Uses SQLAlchemy 2.0 style throughout. The engine and session factory are
created lazily on first use so that tests can configure the singleton before
the engine URL is resolved.
"""
from collections.abc import Generator

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class PipelineBase(DeclarativeBase):
    pass


_pipeline_engine: Engine | None = None
_PipelineSessionLocal: sessionmaker[Session] | None = None


def _get_engine() -> Engine:
    global _pipeline_engine
    if _pipeline_engine is None:
        from .config import get_config

        _pipeline_engine = create_engine(get_config().database.pipeline_db_url)
    return _pipeline_engine


def _get_session_factory() -> sessionmaker[Session]:
    global _PipelineSessionLocal
    if _PipelineSessionLocal is None:
        _PipelineSessionLocal = sessionmaker(_get_engine())
    return _PipelineSessionLocal


@property  # type: ignore[misc]
def pipeline_engine() -> Engine:  # noqa: D401
    """Module-level property shim for backward compatibility."""
    return _get_engine()


def PipelineSessionLocal() -> Session:
    """Return a new pipeline DB session."""
    return _get_session_factory()()


def get_pipeline_db() -> Generator[Session, None, None]:
    """Context-manager / FastAPI dependency for pipeline DB sessions."""
    db = _get_session_factory()()
    try:
        yield db
    finally:
        db.close()


def init_pipeline_db() -> None:
    """Create all pipeline database tables (idempotent)."""
    PipelineBase.metadata.create_all(_get_engine())


def reset_engine() -> None:
    """Reset the engine and session factory singletons.

    Use only in tests when you need to point to a different database URL.
    """
    global _pipeline_engine, _PipelineSessionLocal
    _pipeline_engine = None
    _PipelineSessionLocal = None
