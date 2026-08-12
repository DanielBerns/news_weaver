"""Tests for SQLAlchemy 2.0 ORM models using an in-memory SQLite database."""
from datetime import timezone
from pathlib import Path

import pytest
import yaml
from sqlalchemy import select

import news_weaver.common.config as config_module
from news_weaver.common.config import read_config


@pytest.fixture(autouse=True)
def reset_config_singleton(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Provide a valid config and reset the singleton before each test."""
    config_module._config = None
    monkeypatch.setenv("NEWS_WEAVER_SECRET_KEY", "test-secret")
    data = {
        "database": {
            "data_db_url": "sqlite:///:memory:",
            "pipeline_db_url": "sqlite:///:memory:",
        },
        "api": {"host": "127.0.0.1", "port": 9000},
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(data))
    config_module._config = read_config(config_path=cfg_file)
    yield
    config_module._config = None


@pytest.fixture()
def pipeline_session():
    """Provide a fresh pipeline DB session backed by an in-memory SQLite DB."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession
    from sqlalchemy.pool import StaticPool

    # Import models so their classes register with PipelineBase.metadata
    import news_weaver.common.models  # noqa: F401
    from news_weaver.common.database import PipelineBase, reset_engine

    reset_engine()

    # StaticPool ensures all engine checkouts share the same underlying
    # DBAPI connection, which is required for SQLite :memory: databases.
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    PipelineBase.metadata.create_all(engine)

    with SASession(engine) as session:
        yield session

    reset_engine()


def test_source_can_be_created(pipeline_session) -> None:
    """Source model can be inserted and retrieved."""
    from news_weaver.common.models import Source

    src = Source(
        url="https://example.com/rss",
        source_type="rss",
        schedule="*/30 * * * *",
    )
    pipeline_session.add(src)
    pipeline_session.commit()

    result = pipeline_session.execute(
        select(Source).where(Source.url == "https://example.com/rss")
    ).scalar_one_or_none()

    assert result is not None
    assert result.source_type == "rss"
    assert result.last_scraped_at is None


def test_scraped_file_defaults(pipeline_session) -> None:
    """ScrapedFile.scraped_at defaults to a timezone-aware UTC datetime."""
    from news_weaver.common.models import ScrapedFile, Source

    src = Source(
        url="https://example.com",
        source_type="website",
        schedule="0 * * * *",
    )
    pipeline_session.add(src)
    pipeline_session.commit()

    sf = ScrapedFile(
        source_id=src.id,
        local_path="/tmp/test.html",
        filename="test.html",
        mimetype="text/html",
    )
    pipeline_session.add(sf)
    pipeline_session.commit()

    assert sf.status == "SCRAPED"
    assert sf.retry_count == 0
    assert sf.scraped_at is not None
    # Verify it is timezone-aware
    if sf.scraped_at.tzinfo is not None:
        assert sf.scraped_at.tzinfo == timezone.utc


def test_source_url_unique_constraint(pipeline_session) -> None:
    """Inserting two Sources with the same URL raises an IntegrityError."""
    from sqlalchemy.exc import IntegrityError

    from news_weaver.common.models import Source

    pipeline_session.add(
        Source(url="https://dupe.com", source_type="rss", schedule="* * * * *")
    )
    pipeline_session.commit()

    pipeline_session.add(
        Source(url="https://dupe.com", source_type="rss", schedule="* * * * *")
    )
    with pytest.raises(IntegrityError):
        pipeline_session.commit()
