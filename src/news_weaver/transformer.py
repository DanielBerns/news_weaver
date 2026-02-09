import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
from news_weaver.common.config import CONFIG, setup_logger
from news_weaver.common.database import PipelineSessionLocal
from news_weaver.common.models import Source, ScrapedFile

# Optional imports
try:
    from bs4 import BeautifulSoup
    from pypdf import PdfReader
    import pytesseract
    from PIL import Image
except ImportError:
    print("Missing dependencies: bs4, pypdf, pytesseract, pillow")
    sys.exit(1)

logger = setup_logger("Transformer")

def extract_text(file_record: ScrapedFile) -> dict:
    """Returns a dict payload suitable for the Loader API based on mimetype."""
    path = file_record.local_path
    mime = file_record.mimetype.lower()
    payload = {"source_file_id": file_record.id, "mimetype": mime}

    if "html" in mime:
        with open(path, "r", errors="ignore") as f:
            soup = BeautifulSoup(f, "html.parser")
        payload["endpoint"] = "articles"
        payload.update({
            "title": soup.title.string if soup.title else "No Title",
            "content": soup.get_text(separator="\n"),
            "language": "en"
        })
    elif "image" in mime:
        payload["endpoint"] = "images"
        img = Image.open(path)
        payload.update({
            "extracted_text": pytesseract.image_to_string(img).strip(),
            "detected_objects": [],
            "image_metadata": {}
        })
    else:
        # Generic document fallback
        payload["endpoint"] = "documents"
        with open(path, "r", errors="ignore") as f:
            payload.update({"filename": file_record.filename, "content": f.read()})

    return payload

def send_to_loader(payload: dict) -> bool:
    api_cfg = CONFIG["api"]
    url = f"http://{api_cfg['host']}:{api_cfg['port']}/{payload.pop('endpoint')}"
    headers = {"X-API-Key": api_cfg["secret_key"]}

    try:
        resp = httpx.post(url, json=payload, headers=headers, timeout=10.0)
        return resp.status_code in (200, 201, 409)
    except Exception as e:
        logger.warning(f"Loader API error: {e}")
        return False

def process_file(file_record, session):
    try:
        # Get URL from source relation
        source = session.query(Source).filter(Source.id == file_record.source_id).first()
        url = source.url if source else "unknown"

        payload = extract_text(file_record)
        payload["url"] = url

        if send_to_loader(payload):
            file_record.status = "PROCESSED_SUCCESSFULLY"
        else:
            file_record.status = "LOAD_FAILED"

    except Exception as e:
        logger.error(f"Failed file {file_record.id}: {e}")
        file_record.status = "TRANSFORM_FAILED"
        file_record.notes = str(e)
    finally:
        session.commit()

def main():
    session = PipelineSessionLocal()
    try:
        pending = session.query(ScrapedFile).filter(
            ScrapedFile.status.in_(["SCRAPED", "LOAD_FAILED"])
        ).limit(50).all()

        if not pending:
            return

        logger.info(f"Processing {len(pending)} files...")

        # Mark processing
        for f in pending: f.status = "PROCESSING"
        session.commit()

        # Simple serial processing for SQLite safety (or use ThreadPool if DB allows)
        for f in pending:
            process_file(f, session)

    finally:
        session.close()

if __name__ == "__main__":
    main()
