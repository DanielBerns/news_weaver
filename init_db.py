import sys
import logging

# Ensure we can import from the src directory if running from project root
sys.path.append('src')

from news_weaver.common.database import PipelineSessionLocal, init_pipeline_db
from news_weaver.common.config import setup_logger
from news_weaver.common.models import Source

# Setup a simple logger for this script
logger = setup_logger("InitDB")

def initialize_database():
    """Creates tables and seeds initial data."""

    # 1. Create Tables (Idempotent)
    logger.info("Ensuring database tables exist...")
    init_pipeline_db()

    # 2. Seed Data
    session = PipelineSessionLocal()
    try:
        # Define the test source
        hacker_news_rss = "https://news.ycombinator.com/rss"

        # Check if it already exists to prevent duplicates
        existing_source = session.query(Source).filter_by(url=hacker_news_rss).first()

        if not existing_source:
            new_source = Source(
                url=hacker_news_rss,
                source_type="rss",
                schedule="*/30 * * * *"  # Run every 30 minutes
            )
            session.add(new_source)
            session.commit()
            logger.info(f"Successfully added seed source: {new_source.url}")
            print(f"Added source: {new_source.url}")
        else:
            logger.info(f"Seed source already exists: {hacker_news_rss}")
            print(f"Source already exists: {hacker_news_rss}")

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        session.rollback()
        sys.exit(1)
    finally:
        session.close()

if __name__ == "__main__":
    initialize_database()
