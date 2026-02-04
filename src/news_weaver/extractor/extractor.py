import argparse
import logging
import os
import sys
import shutil
import mimetypes
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
import yaml
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Text
from sqlalchemy.orm import sessionmaker, declarative_base, relationship

# --- Configuration & Logging ---

def load_config(config_path: str = "config.yaml") -> dict:
    if not os.path.exists(config_path):
        print(f"Error: {config_path} not found.")
        sys.exit(1)
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

# Ensure output directory exists
SCRAPED_DATA_DIR = CONFIG["system"].get("scraped_data_dir", "./scraped_data")
os.makedirs(SCRAPED_DATA_DIR, exist_ok=True)

logging.basicConfig(
    filename=CONFIG["logging"]["file"],
    level=getattr(logging, CONFIG["logging"]["level"].upper(), logging.INFO),
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "Extractor", "message": "%(message)s"}'
)
logger = logging.getLogger("Extractor")

# --- Database Models (pipeline.db) ---

DATABASE_URL = CONFIG["database"]["pipeline_db_url"]
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class Source(Base):
    __tablename__ = "sources"
    id = Column(Integer, primary_key=True)
    url = Column(String, unique=True, nullable=False)
    source_type = Column(String, nullable=False)
    schedule = Column(String, nullable=False)
    last_scraped_at = Column(DateTime, nullable=True)

class ScrapedFile(Base):
    __tablename__ = "scraped_files"
    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey("sources.id"), nullable=False)
    local_path = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    mimetype = Column(String, nullable=False)
    scraped_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    status = Column(String, default="SCRAPED")  # SCRAPED, PROCESSING, PROCESSED, FAILED
    retry_count = Column(Integer, default=0)
    notes = Column(Text, nullable=True)

# Ensure tables exist
Base.metadata.create_all(engine)

# --- Core Logic ---

def get_filename_from_response(response, url):
    """Attempts to derive a filename from Content-Disposition or URL."""
    # 1. Try Content-Disposition
    # (Simplified for brevity; robust parsing usually requires 'cgi.parse_header')
    if "Content-Disposition" in response.headers:
        cd = response.headers["Content-Disposition"]
        if "filename=" in cd:
            return cd.split("filename=")[-1].strip('"')

    # 2. Try URL path
    path = urlparse(url).path
    name = os.path.basename(path)
    if name:
        return name

    # 3. Fallback
    return "index.html"

def save_content(source_id: int, content: bytes, filename: str) -> str:
    """Saves bytes to disk with a collision-resistant name. Returns absolute path."""
    timestamp = int(time.time())
    safe_filename = f"{source_id}_{timestamp}_{filename}"
    file_path = os.path.join(SCRAPED_DATA_DIR, safe_filename)

    with open(file_path, "wb") as f:
        f.write(content)

    return os.path.abspath(file_path)

def process_http_source(session, source: Source):
    """Handles HTTP/HTTPS sources."""
    try:
        logger.info(f"Fetching URL: {source.url}")

        # Follow redirects, set a reasonable timeout
        with httpx.Client(follow_redirects=True, timeout=30.0) as client:
            resp = client.get(source.url)

            if resp.status_code >= 400:
                logger.warning(f"HTTP Error {resp.status_code} for {source.url}")
                # We do NOT create a ScrapedFile record for transient HTTP errors
                # to allow the cron to retry later naturally.
                # Fatal errors (404/403) could be logged to specific error tables if desired.
                return

            # Determine Metadata
            content_type = resp.headers.get("Content-Type", "application/octet-stream").split(";")[0]
            filename = get_filename_from_response(resp, source.url)

            # Save File
            saved_path = save_content(source.id, resp.content, filename)

            # Record in DB
            new_file = ScrapedFile(
                source_id=source.id,
                local_path=saved_path,
                filename=filename,
                mimetype=content_type,
                status="SCRAPED"
            )
            session.add(new_file)

            # Update Source
            source.last_scraped_at = datetime.now(timezone.utc)
            session.commit()

            logger.info(f"Successfully scraped {source.url} -> {saved_path}")

    except httpx.RequestError as e:
        logger.error(f"Network error scraping {source.url}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error processing HTTP source {source.id}: {e}")

def process_local_source(session, source: Source):
    """Handles local FILE:// sources (directories)."""
    try:
        # Parse path from URL (file:///path/to/dir -> /path/to/dir)
        parsed = urlparse(source.url)
        local_dir = parsed.path

        if not os.path.exists(local_dir):
            logger.error(f"Local path does not exist: {local_dir}")
            return

        # Iterate through files in directory
        for entry in os.scandir(local_dir):
            if entry.is_file():
                # Check if we already scraped this specific file modification
                # We use file size + mtime as a simple signature
                file_stat = entry.stat()
                signature = f"size={file_stat.st_size}_mtime={file_stat.st_mtime}"

                # Check DB for duplicate (naive check, can be improved with hashing)
                # We check the 'notes' field or similar if we stored the signature,
                # but for now, we'll just check if filename was scraped recently?
                # Simpler: Always import, Transformer dedupes?
                # BETTER: Check if we have a ScrapedFile for this source/filename
                # that is PROCESSED. If so, check if file is newer.
                # For this MVP, we will ingest it if it's not in the DB as 'SCRAPED' or 'PROCESSING'.

                existing = session.query(ScrapedFile).filter(
                    ScrapedFile.source_id == source.id,
                    ScrapedFile.filename == entry.name,
                    ScrapedFile.status.in_(["SCRAPED", "PROCESSING"])
                ).first()

                if existing:
                    continue # Still processing previous version

                # Detect Mimetype
                mime_type, _ = mimetypes.guess_type(entry.path)
                if not mime_type:
                    mime_type = "application/octet-stream"

                # Read and Copy
                with open(entry.path, "rb") as f:
                    content = f.read()

                saved_path = save_content(source.id, content, entry.name)

                new_file = ScrapedFile(
                    source_id=source.id,
                    local_path=saved_path,
                    filename=entry.name,
                    mimetype=mime_type,
                    status="SCRAPED",
                    notes=signature # Store signature to help future deduping logic
                )
                session.add(new_file)
                logger.info(f"Ingested local file: {entry.name}")

        source.last_scraped_at = datetime.now(timezone.utc)
        session.commit()

    except Exception as e:
        logger.error(f"Error processing local source {source.id}: {e}")

def main():
    parser = argparse.ArgumentParser(description="ETL Extractor")
    parser.add_argument("--source_id", type=int, required=True, help="ID of the source to scrape")
    args = parser.parse_args()

    session = SessionLocal()
    try:
        source = session.query(Source).filter(Source.id == args.source_id).first()
        if not source:
            logger.error(f"Source ID {args.source_id} not found.")
            sys.exit(1)

        logger.info(f"Starting job for Source {source.id} [{source.source_type}]")

        if source.source_type.lower() in ["website", "rss", "http", "https"]:
            process_http_source(session, source)
        elif source.source_type.lower() in ["local", "file"]:
            process_local_source(session, source)
        else:
            logger.error(f"Unknown source type: {source.source_type}")

    except Exception as e:
        logger.critical(f"Critical failure in Extractor: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    main()
