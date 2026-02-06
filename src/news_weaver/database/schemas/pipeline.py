"""Pydantic schemas for pipeline.db models."""
from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime

class SourceBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    url: str = Field(...)
    source_type: str = Field(..., pattern="^(web|rss|file)$")
    schedule: str = Field(...)
    active: int = Field(default=1, ge=0, le=1)

class SourceCreate(SourceBase):
    pass

class SourceRead(SourceBase):
    id: int
    created_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

class ScrapedFileBase(BaseModel):
    source_id: int
    local_path: str
    filename: str
    mimetype: str
    status: str = Field(default="SCRAPED")
    retry_count: int = Field(default=0, ge=0)

class ScrapedFileCreate(ScrapedFileBase):
    pass

class ScrapedFileRead(ScrapedFileBase):
    id: int
    scraped_at: Optional[datetime] = None
    notes: Optional[str] = None
    
    class Config:
        from_attributes = True

class ScrapedFileUpdate(BaseModel):
    status: Optional[str] = None
    retry_count: Optional[int] = None
    notes: Optional[str] = None