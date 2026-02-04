import json
import logging
import os
import sys
from datetime import datetime
from typing import List, Optional, Dict, Any

import yaml
from fastapi import FastAPI, HTTPException, Header, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base, Session

# --- Configuration & Logging Setup ---

def load_config(config_path: str = "./config.yaml") -> dict:
    """Loads configuration from a YAML file."""
    if not os.path.exists(config_path):
        # Fallback for demonstration if file doesn't exist
        print(f"Warning: {config_path} not found. Using defaults.")
        return {
            "database": {"data_db_url": "sqlite:///data.db"},
            "api": {"host": "127.0.0.1", "port": 8000, "secret_key": "supersecretkey"},
            "logging": {"file": "pipeline.log", "level": "INFO"}
        }
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

# Setup Structured Logging
logging.basicConfig(
    filename=CONFIG["logging"]["file"],
    level=getattr(logging, CONFIG["logging"]["level"].upper(), logging.INFO),
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "Loader", "message": "%(message)s"}'
)
logger = logging.getLogger("Loader")

# --- Database Setup (data.db) ---

DATABASE_URL = CONFIG["database"]["data_db_url"]
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- SQLAlchemy Models ---

class Article(Base):
    __tablename__ = "articles"
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, unique=True, index=True, nullable=False)
    url = Column(String, nullable=False)
    title = Column(String, nullable=True)
    content = Column(Text, nullable=False)
    language = Column(String, nullable=True)
    ingested_at = Column(DateTime, default=datetime.utcnow)

class Document(Base):
    __tablename__ = "documents"
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, unique=True, index=True, nullable=False)
    url = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    mimetype = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    ingested_at = Column(DateTime, default=datetime.utcnow)

class Spreadsheet(Base):
    __tablename__ = "spreadsheets"
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, unique=True, index=True, nullable=False)
    url = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    mimetype = Column(String, nullable=False)
    data_json = Column(Text, nullable=False)
    ingested_at = Column(DateTime, default=datetime.utcnow)

class Image(Base):
    __tablename__ = "images"
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, unique=True, index=True, nullable=False)
    url = Column(String, nullable=False)
    mimetype = Column(String, nullable=False)

    extracted_text = Column(Text, nullable=True)
    detected_objects = Column(Text, nullable=True)

    # RENAMED: 'metadata' is reserved by SQLAlchemy, so we use 'image_metadata'
    image_metadata = Column(Text, nullable=True)

    ingested_at = Column(DateTime, default=datetime.utcnow)

# Create Tables
Base.metadata.create_all(bind=engine)

# --- Pydantic Schemas (Input Validation) ---

class BasePayload(BaseModel):
    source_file_id: int
    url: str

class ArticleCreate(BasePayload):
    title: Optional[str] = None
    content: str
    language: Optional[str] = "en"

class DocumentCreate(BasePayload):
    filename: str
    mimetype: str
    content: str

class SpreadsheetCreate(BasePayload):
    filename: str
    mimetype: str
    data_json: List[Dict[str, Any]]

class ImageCreate(BasePayload):
    mimetype: str
    extracted_text: Optional[str] = ""
    detected_objects: Optional[List[str]] = []
    # RENAMED: Matching the database model for clarity
    image_metadata: Optional[Dict[str, Any]] = {}

# --- Dependency Injection ---

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def verify_api_key(x_api_key: str = Header(...)):
    """Enforces API Key authentication."""
    if x_api_key != CONFIG["api"]["secret_key"]:
        logger.warning(f"Unauthorized access attempt. Key provided: {x_api_key[:4]}***")
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return x_api_key

# --- FastAPI App & Routes ---

app = FastAPI(title="ETL Loader API", version="1.0.0")

@app.post("/articles", status_code=201, dependencies=[Depends(verify_api_key)])
def create_article(item: ArticleCreate, db: Session = Depends(get_db)):
    if db.query(Article).filter(Article.source_file_id == item.source_file_id).first():
        logger.info(f"Duplicate article skipped. ID: {item.source_file_id}")
        return {"message": "Already exists", "id": item.source_file_id}

    db_article = Article(
        source_file_id=item.source_file_id,
        url=item.url,
        title=item.title,
        content=item.content,
        language=item.language
    )
    try:
        db.add(db_article)
        db.commit()
        db.refresh(db_article)
        logger.info(f"Article loaded successfully. ID: {item.source_file_id}")
        return {"status": "success", "db_id": db_article.id}
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to save article {item.source_file_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Database error")

@app.post("/documents", status_code=201, dependencies=[Depends(verify_api_key)])
def create_document(item: DocumentCreate, db: Session = Depends(get_db)):
    if db.query(Document).filter(Document.source_file_id == item.source_file_id).first():
        return {"message": "Already exists", "id": item.source_file_id}

    db_doc = Document(
        source_file_id=item.source_file_id,
        url=item.url,
        filename=item.filename,
        mimetype=item.mimetype,
        content=item.content
    )
    try:
        db.add(db_doc)
        db.commit()
        logger.info(f"Document loaded successfully. ID: {item.source_file_id}")
        return {"status": "success"}
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to save document {item.source_file_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Database error")

@app.post("/spreadsheets", status_code=201, dependencies=[Depends(verify_api_key)])
def create_spreadsheet(item: SpreadsheetCreate, db: Session = Depends(get_db)):
    if db.query(Spreadsheet).filter(Spreadsheet.source_file_id == item.source_file_id).first():
        return {"message": "Already exists", "id": item.source_file_id}

    json_str = json.dumps(item.data_json)

    db_sheet = Spreadsheet(
        source_file_id=item.source_file_id,
        url=item.url,
        filename=item.filename,
        mimetype=item.mimetype,
        data_json=json_str
    )
    try:
        db.add(db_sheet)
        db.commit()
        logger.info(f"Spreadsheet loaded successfully. ID: {item.source_file_id}")
        return {"status": "success"}
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to save spreadsheet {item.source_file_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Database error")

@app.post("/images", status_code=201, dependencies=[Depends(verify_api_key)])
def create_image(item: ImageCreate, db: Session = Depends(get_db)):
    if db.query(Image).filter(Image.source_file_id == item.source_file_id).first():
        return {"message": "Already exists", "id": item.source_file_id}

    objects_json = json.dumps(item.detected_objects) if item.detected_objects else "[]"
    metadata_json = json.dumps(item.image_metadata) if item.image_metadata else "{}"

    db_image = Image(
        source_file_id=item.source_file_id,
        url=item.url,
        mimetype=item.mimetype,
        extracted_text=item.extracted_text,
        detected_objects=objects_json,
        image_metadata=metadata_json
    )
    try:
        db.add(db_image)
        db.commit()
        logger.info(f"Image loaded successfully. ID: {item.source_file_id}")
        return {"status": "success"}
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to save image {item.source_file_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Database error")

if __name__ == "__main__":
    import uvicorn
    host = CONFIG["api"].get("host", "127.0.0.1")
    port = CONFIG["api"].get("port", 8000)
    print(f"Starting Loader API on {host}:{port}...")
    uvicorn.run(app, host=host, port=port)
