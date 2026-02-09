import argparse
import sys
import os
import time
import httpx
import mimetypes
from urllib.parse import urlparse
from datetime import datetime, timezone

from news_weaver.common.config import CONFIG, setup_logger
from news_weaver.common.database import PipelineSessionLocal
from news_weaver.common.models import Source, ScrapedFile

logger = setup_logger("Extractor")
SCRAPED_DATA_DIR = CONFIG["system"].get("scraped_data_dir", "./scraped_data")
os.makedirs(SCRAPED_DATA_DIR, exist_ok=True)

def save_content(source_id: int, content: bytes, filename: str) -> str:
    timestamp = int(time.time())
    safe_filename = f"{source_id}_{timestamp}_{filename}"
    file_path = os.path.join(SCRAPED_DATA_DIR, safe_filename)
    with open(file_path, "wb") as f:
        f.write(content)
    return os.path.abspath(file_path)

def process_http_source(session, source: Source):
    try:
        logger.info(f"Fetching: {source.url}")
        with httpx.Client(follow_redirects=True, timeout=30.0) as client:
            resp = client.get(source.url)
            if resp.status_code >= 400:
                logger.warning(f"HTTP {resp.status_code} for {source.url}")
                return

            filename = os.path.basename(urlparse(source.url).path) or "index.html"
            content_type = resp.headers.get("Content-Type", "application/octet-stream").split(";")[0]
            saved_path = save_content(source.id, resp.content, filename)

            new_file = ScrapedFile(
                source_id=source.id, local_path=saved_path, filename=filename,
                mimetype=content_type, status="SCRAPED"
            )
            session.add(new_file)
            source.last_scraped_at = datetime.now(timezone.utc)
            session.commit()
            logger.info(f"Scraped {source.url}")

    except Exception as e:
        logger.error(f"Error scraping {source.url}: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_id", type=int, required=True)
    args = parser.parse_args()

    session = PipelineSessionLocal()
    try:
        source = session.query(Source).filter(Source.id == args.source_id).first()
        if not source:
            logger.error(f"Source {args.source_id} not found.")
            sys.exit(1)

        if source.source_type.lower() in ["website", "rss", "http", "https"]:
            process_http_source(session, source)
        # Add other types (local/file) here as needed
        else:
            logger.error(f"Unknown source type: {source.source_type}")

    finally:
        session.close()

if __name__ == "__main__":
    main()
