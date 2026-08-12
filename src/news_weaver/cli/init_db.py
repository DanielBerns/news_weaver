"""CLI entry-point: initialise and seed the pipeline database.

Run via:
    uv run init-db
"""
import sys

from sqlalchemy import select

from news_weaver.common.config import setup_logger
from news_weaver.common.database import PipelineSessionLocal, init_pipeline_db
from news_weaver.common.models import Source

logger = setup_logger("InitDB")


def main() -> None:
    """Create tables and seed initial data."""
    logger.info("Ensuring database tables exist...")
    init_pipeline_db()

    session = PipelineSessionLocal()
    try:
        hacker_news_rss = "https://news.ycombinator.com/rss"

        existing = session.execute(
            select(Source).where(Source.url == hacker_news_rss)
        ).scalar_one_or_none()

        if not existing:
            new_source = Source(
                url=hacker_news_rss,
                source_type="rss",
                schedule="*/30 * * * *",  # every 30 minutes
            )
            session.add(new_source)
            session.commit()
            logger.info(f"Added seed source: {new_source.url}")
            print(f"Added source: {new_source.url}")
        else:
            logger.info(f"Seed source already exists: {hacker_news_rss}")
            print(f"Source already exists: {hacker_news_rss}")

    except Exception as e:
        logger.error(f"Database initialisation failed: {e}")
        session.rollback()
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
