"""Transformer — processes staged raw files and pushes data to the Loader API.

Uses SQLAlchemy 2.0 select() queries and get_config() singleton.
"""
import sys
from typing import Any

import httpx
from sqlalchemy import select

from news_weaver.common.config import get_config, setup_logger
from news_weaver.common.database import PipelineSessionLocal
from news_weaver.common.models import ScrapedFile, Source

# Optional imports — fail fast with a helpful message
try:
    import pytesseract
    from bs4 import BeautifulSoup
    from PIL import Image
except ImportError:
    print("Missing dependencies: bs4, pypdf, pytesseract, pillow")
    sys.exit(1)

logger = setup_logger("Transformer")


def extract_text(file_record: ScrapedFile) -> dict[str, Any]:
    """Return a payload dict for the Loader API based on mimetype."""
    path = file_record.local_path
    mime = file_record.mimetype.lower()
    payload: dict[str, Any] = {
        "source_file_id": file_record.id,
        "mimetype": mime,
    }

    if "html" in mime:
        with open(path, "r", errors="ignore") as fh:
            soup = BeautifulSoup(fh, "html.parser")
        payload["endpoint"] = "articles"
        payload.update(
            {
                "title": soup.title.string if soup.title else "No Title",
                "content": soup.get_text(separator="\n"),
                "language": "en",
            }
        )
    elif "image" in mime:
        payload["endpoint"] = "images"
        img = Image.open(path)
        payload.update(
            {
                "extracted_text": pytesseract.image_to_string(img).strip(),
                "detected_objects": [],
                "image_metadata": {},
            }
        )
    else:
        # Generic document fallback
        payload["endpoint"] = "documents"
        with open(path, "r", errors="ignore") as fh:
            payload.update({"filename": file_record.filename, "content": fh.read()})

    return payload


def send_to_loader(payload: dict[str, Any]) -> bool:
    api_cfg = get_config().api
    endpoint = payload.pop("endpoint")
    url = f"http://{api_cfg.host}:{api_cfg.port}/{endpoint}"
    headers = {"X-API-Key": api_cfg.secret_key}

    try:
        resp = httpx.post(url, json=payload, headers=headers, timeout=10.0)
        return resp.status_code in (200, 201, 409)
    except Exception as e:
        logger.warning(f"Loader API error: {e}")
        return False


def process_file(file_record: ScrapedFile, session: Any) -> None:
    try:
        source = session.execute(
            select(Source).where(Source.id == file_record.source_id)
        ).scalar_one_or_none()
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


def main() -> None:
    session = PipelineSessionLocal()
    try:
        pending = list(
            session.execute(
                select(ScrapedFile).where(
                    ScrapedFile.status.in_(["SCRAPED", "LOAD_FAILED"])
                ).limit(50)
            ).scalars()
        )

        if not pending:
            return

        logger.info(f"Processing {len(pending)} files...")

        # Mark as processing in one commit
        for f in pending:
            f.status = "PROCESSING"
        session.commit()

        # Serial processing for SQLite safety
        for f in pending:
            process_file(f, session)

    finally:
        session.close()


if __name__ == "__main__":
    main()
