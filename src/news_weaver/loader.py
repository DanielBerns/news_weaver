"""FastAPI Loader service — the ETL pipeline's destination.

Validates incoming data and persists it to the data warehouse database.
Uses SQLAlchemy 2.0 style throughout. Database engine is created lazily
on first use to allow tests to configure the config singleton first.
"""
import json
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel
from sqlalchemy import Engine, String, Text, create_engine, select
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    sessionmaker,
)

from news_weaver.common.config import get_config, setup_logger

logger = setup_logger("Loader")


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI) -> AsyncGenerator[None, None]:
    """Create DB tables on startup."""
    Base.metadata.create_all(_get_loader_engine())
    yield


app = FastAPI(title="ETL Loader API", lifespan=lifespan)

# ---------------------------------------------------------------------------
# Database setup (data warehouse) — lazy initialization
# ---------------------------------------------------------------------------

_engine: Engine | None = None
_SessionLocal: sessionmaker[Session] | None = None


def _get_loader_engine() -> Engine:
    global _engine
    if _engine is None:
        url = get_config().database.data_db_url
        # check_same_thread=False is required for SQLite + multithreaded FastAPI
        _engine = create_engine(url, connect_args={"check_same_thread": False})
    return _engine


def _get_loader_session_factory() -> sessionmaker[Session]:
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(_get_loader_engine())
    return _SessionLocal


def reset_loader_engine() -> None:
    """Reset loader engine and session singletons. Use only in tests."""
    global _engine, _SessionLocal
    _engine = None
    _SessionLocal = None


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# ORM Models (SQLAlchemy 2.0 style)
# ---------------------------------------------------------------------------


class Article(Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    source_file_id: Mapped[int] = mapped_column(unique=True, index=True)
    url: Mapped[str] = mapped_column(String)
    title: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    content: Mapped[str] = mapped_column(Text)
    language: Mapped[str] = mapped_column(String, default="en")
    ingested_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    source_file_id: Mapped[int] = mapped_column(unique=True, index=True)
    url: Mapped[str] = mapped_column(String)
    filename: Mapped[str] = mapped_column(String)
    mimetype: Mapped[str] = mapped_column(String)
    content: Mapped[str] = mapped_column(Text)
    ingested_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )


class Spreadsheet(Base):
    __tablename__ = "spreadsheets"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    source_file_id: Mapped[int] = mapped_column(unique=True, index=True)
    url: Mapped[str] = mapped_column(String)
    filename: Mapped[str] = mapped_column(String)
    mimetype: Mapped[str] = mapped_column(String)
    data_json: Mapped[str] = mapped_column(Text)  # stored as JSON string
    ingested_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )


class Image(Base):
    __tablename__ = "images"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    source_file_id: Mapped[int] = mapped_column(unique=True, index=True)
    url: Mapped[str] = mapped_column(String)
    mimetype: Mapped[str] = mapped_column(String)
    extracted_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    detected_objects: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON
    image_metadata: Mapped[Optional[str]] = mapped_column(Text, nullable=True)   # JSON
    ingested_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )


# ---------------------------------------------------------------------------
# Pydantic schemas (input validation)
# ---------------------------------------------------------------------------


class BasePayload(BaseModel):
    source_file_id: int
    url: str


class ArticleCreate(BasePayload):
    title: Optional[str] = None
    content: str
    language: Optional[str] = "en"


class DocumentCreate(BasePayload):
    filename: str = "unknown"
    mimetype: str = "text/plain"
    content: str


class SpreadsheetCreate(BasePayload):
    filename: str
    mimetype: str
    data_json: List[Dict[str, Any]]


class ImageCreate(BasePayload):
    mimetype: str
    extracted_text: Optional[str] = ""
    detected_objects: Optional[List[str]] = []
    image_metadata: Optional[Dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


def get_db() -> Generator[Session, None, None]:
    db = _get_loader_session_factory()()
    try:
        yield db
    finally:
        db.close()


def verify_key(x_api_key: str = Header(...)) -> str:
    if x_api_key != get_config().api.secret_key:
        logger.warning("Unauthorized access attempt.")
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return x_api_key


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------


@app.post("/articles", status_code=201, dependencies=[Depends(verify_key)])
def create_article(
    item: ArticleCreate, db: Session = Depends(get_db)
) -> dict[str, object]:
    if db.execute(
        select(Article).where(Article.source_file_id == item.source_file_id)
    ).scalar_one_or_none():
        return {"status": "exists", "id": item.source_file_id}

    db.add(Article(**item.model_dump()))
    db.commit()
    logger.info(f"Loaded Article {item.source_file_id}")
    return {"status": "success"}


@app.post("/documents", status_code=201, dependencies=[Depends(verify_key)])
def create_document(
    item: DocumentCreate, db: Session = Depends(get_db)
) -> dict[str, object]:
    if db.execute(
        select(Document).where(Document.source_file_id == item.source_file_id)
    ).scalar_one_or_none():
        return {"status": "exists", "id": item.source_file_id}

    db.add(Document(**item.model_dump()))
    db.commit()
    logger.info(f"Loaded Document {item.source_file_id}")
    return {"status": "success"}


@app.post("/spreadsheets", status_code=201, dependencies=[Depends(verify_key)])
def create_spreadsheet(
    item: SpreadsheetCreate, db: Session = Depends(get_db)
) -> dict[str, object]:
    if db.execute(
        select(Spreadsheet).where(Spreadsheet.source_file_id == item.source_file_id)
    ).scalar_one_or_none():
        return {"status": "exists", "id": item.source_file_id}

    data = item.model_dump()
    data["data_json"] = json.dumps(item.data_json)
    db.add(Spreadsheet(**data))
    db.commit()
    logger.info(f"Loaded Spreadsheet {item.source_file_id}")
    return {"status": "success"}


@app.post("/images", status_code=201, dependencies=[Depends(verify_key)])
def create_image(
    item: ImageCreate, db: Session = Depends(get_db)
) -> dict[str, object]:
    if db.execute(
        select(Image).where(Image.source_file_id == item.source_file_id)
    ).scalar_one_or_none():
        return {"status": "exists", "id": item.source_file_id}

    data = item.model_dump()
    data["detected_objects"] = json.dumps(item.detected_objects)
    data["image_metadata"] = json.dumps(item.image_metadata)
    db.add(Image(**data))
    db.commit()
    logger.info(f"Loaded Image {item.source_file_id}")
    return {"status": "success"}


if __name__ == "__main__":
    import uvicorn

    cfg = get_config()
    uvicorn.run(app, host=cfg.api.host, port=cfg.api.port)
