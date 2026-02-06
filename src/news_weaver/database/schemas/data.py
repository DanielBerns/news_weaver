"""Pydantic schemas for data.db models."""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

class ArticleBase(BaseModel):
    title: str = Field(..., min_length=1, max_length=500)
    content: str = Field(..., min_length=1)
    language: Optional[str] = "en"

class ArticleCreate(ArticleBase):
    source_file_id: int
    url: str

class ArticleRead(ArticleBase):
    id: int
    ingested_at: datetime
    
    class Config:
        from_attributes = True

class DocumentBase(BaseModel):
    filename: str
    mimetype: str
    content: str

class DocumentCreate(DocumentBase):
    source_file_id: int
    url: str

class DocumentRead(DocumentBase):
    id: int
    ingested_at: datetime
    
    class Config:
        from_attributes = True

class SpreadsheetBase(BaseModel):
    filename: str
    mimetype: str
    data_json: List[Dict[str, Any]]

class SpreadsheetCreate(SpreadsheetBase):
    source_file_id: int
    url: str

class SpreadsheetRead(SpreadsheetBase):
    id: int
    ingested_at: datetime
    
    class Config:
        from_attributes = True

class ImageBase(BaseModel):
    mimetype: str
    extracted_text: Optional[str] = ""
    detected_objects: Optional[List[str]] = []
    image_metadata: Optional[Dict[str, Any]] = {}

class ImageCreate(ImageBase):
    source_file_id: int
    url: str

class ImageRead(ImageBase):
    id: int
    ingested_at: datetime
    
    class Config:
        from_attributes = True