from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text
from datetime import datetime, timezone
from .database import PipelineBase

class Source(PipelineBase):
    __tablename__ = "sources"
    id = Column(Integer, primary_key=True)
    url = Column(String, unique=True, nullable=False)
    source_type = Column(String, nullable=False)  # rss, website, local
    schedule = Column(String, nullable=False)     # cron expression
    last_scraped_at = Column(DateTime, nullable=True)

class ScrapedFile(PipelineBase):
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
