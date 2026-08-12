"""Extractor — fetches raw content from configured sources.

Saves content to a local staging directory and records metadata in the
pipeline database. Uses SQLAlchemy 2.0 select() queries and pathlib.
"""
import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx
from sqlalchemy import select

from news_weaver.common.config import get_config, setup_logger
from news_weaver.common.database import PipelineSessionLocal
from news_weaver.common.models import ScrapedFile, Source

logger = setup_logger("Extractor")

_cfg = get_config()
SCRAPED_DATA_DIR: Path = _cfg.scraped_data_path
SCRAPED_DATA_DIR.mkdir(parents=True, exist_ok=True)


def save_content(source_id: int, content: bytes, filename: str) -> str:
    """Save raw bytes to the staging directory; return the absolute path."""
    timestamp = int(time.time())
    safe_filename = f"{source_id}_{timestamp}_{filename}"
    file_path = SCRAPED_DATA_DIR / safe_filename
    file_path.write_bytes(content)
    return str(file_path.resolve())


def process_http_source(session: object, source: Source) -> None:
    try:
        logger.info(f"Fetching: {source.url}")
        with httpx.Client(follow_redirects=True, timeout=30.0) as client:
            resp = client.get(source.url)
            if resp.status_code >= 400:
                logger.warning(f"HTTP {resp.status_code} for {source.url}")
                return

            filename = Path(urlparse(source.url).path).name or "index.html"
            content_type = (
                resp.headers.get("Content-Type", "application/octet-stream")
                .split(";")[0]
                .strip()
            )
            saved_path = save_content(source.id, resp.content, filename)

            new_file = ScrapedFile(
                source_id=source.id,
                local_path=saved_path,
                filename=filename,
                mimetype=content_type,
                status="SCRAPED",
            )
            session.add(new_file)  # type: ignore[attr-defined]
            source.last_scraped_at = datetime.now(timezone.utc)
            session.commit()  # type: ignore[attr-defined]
            logger.info(f"Scraped {source.url}")

    except Exception as e:
        logger.error(f"Error scraping {source.url}: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="news_weaver extractor")
    parser.add_argument("--source_id", type=int, required=True)
    args = parser.parse_args()

    session = PipelineSessionLocal()
    try:
        source = session.execute(
            select(Source).where(Source.id == args.source_id)
        ).scalar_one_or_none()

        if not source:
            logger.error(f"Source {args.source_id} not found.")
            sys.exit(1)

        if source.source_type.lower() in ("website", "rss", "http", "https"):
            process_http_source(session, source)
        else:
            logger.error(f"Unknown source type: {source.source_type}")

    finally:
        session.close()


if __name__ == "__main__":
    main()
