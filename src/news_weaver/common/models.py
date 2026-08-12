"""SQLAlchemy 2.0 ORM models for the pipeline database."""
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .database import PipelineBase


class Source(PipelineBase):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    # rss, website, local
    source_type: Mapped[str] = mapped_column(String, nullable=False)
    schedule: Mapped[str] = mapped_column(String, nullable=False)  # cron expression
    last_scraped_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)


class ScrapedFile(PipelineBase):
    __tablename__ = "scraped_files"

    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)
    local_path: Mapped[str] = mapped_column(String, nullable=False)
    filename: Mapped[str] = mapped_column(String, nullable=False)
    mimetype: Mapped[str] = mapped_column(String, nullable=False)
    scraped_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )
    status: Mapped[str] = mapped_column(
        String, default="SCRAPED"
    )  # SCRAPED, PROCESSING, PROCESSED, FAILED
    retry_count: Mapped[int] = mapped_column(default=0)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
