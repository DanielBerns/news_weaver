import json
import logging
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from datetime import datetime

# Import shared config
from news_weaver.common.config import CONFIG, setup_logger

logger = setup_logger("Loader")
app = FastAPI(title="ETL Loader API")

# --- Data Database Setup ---
DATA_DB_URL = CONFIG["database"]["data_db_url"]
# check_same_thread=False is needed for SQLite
engine = create_engine(DATA_DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# --- Database Models ---

class Article(Base):
    __tablename__ = "articles"
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, unique=True, index=True)
    url = Column(String)
    title = Column(String, nullable=True)
    content = Column(Text)
    language = Column(String, default="en")
    ingested_at = Column(DateTime, default=datetime.utcnow)

class Document(Base):
    __tablename__ = "documents"
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, unique=True, index=True)
    url = Column(String)
    filename = Column(String)
    mimetype = Column(String)
    content = Column(Text)
    ingested_at = Column(DateTime, default=datetime.utcnow)

class Spreadsheet(Base):
    __tablename__ = "spreadsheets"
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, unique=True, index=True)
    url = Column(String)
    filename = Column(String)
    mimetype = Column(String)
    data_json = Column(Text)  # stored as JSON string
    ingested_at = Column(DateTime, default=datetime.utcnow)

class Image(Base):
    __tablename__ = "images"
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, unique=True, index=True)
    url = Column(String)
    mimetype = Column(String)
    extracted_text = Column(Text, nullable=True)
    detected_objects = Column(Text, nullable=True) # stored as JSON string
    image_metadata = Column(Text, nullable=True)   # stored as JSON string
    ingested_at = Column(DateTime, default=datetime.utcnow)

# Create all tables
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

# --- Dependencies ---

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def verify_key(x_api_key: str = Header(...)):
    if x_api_key != CONFIG["api"]["secret_key"]:
        logger.warning(f"Unauthorized access attempt.")
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return x_api_key

# --- API Endpoints ---

@app.post("/articles", status_code=201, dependencies=[Depends(verify_key)])
def create_article(item: ArticleCreate, db: Session = Depends(get_db)):
    if db.query(Article).filter(Article.source_file_id == item.source_file_id).first():
        return {"status": "exists", "id": item.source_file_id}

    new_obj = Article(**item.dict())
    db.add(new_obj)
    db.commit()
    logger.info(f"Loaded Article {item.source_file_id}")
    return {"status": "success"}

@app.post("/documents", status_code=201, dependencies=[Depends(verify_key)])
def create_document(item: DocumentCreate, db: Session = Depends(get_db)):
    if db.query(Document).filter(Document.source_file_id == item.source_file_id).first():
        return {"status": "exists", "id": item.source_file_id}

    new_obj = Document(**item.dict())
    db.add(new_obj)
    db.commit()
    logger.info(f"Loaded Document {item.source_file_id}")
    return {"status": "success"}

@app.post("/spreadsheets", status_code=201, dependencies=[Depends(verify_key)])
def create_spreadsheet(item: SpreadsheetCreate, db: Session = Depends(get_db)):
    if db.query(Spreadsheet).filter(Spreadsheet.source_file_id == item.source_file_id).first():
        return {"status": "exists", "id": item.source_file_id}

    # Convert list/dict to JSON string for storage
    data = item.dict()
    data['data_json'] = json.dumps(item.data_json)

    new_obj = Spreadsheet(**data)
    db.add(new_obj)
    db.commit()
    logger.info(f"Loaded Spreadsheet {item.source_file_id}")
    return {"status": "success"}

@app.post("/images", status_code=201, dependencies=[Depends(verify_key)])
def create_image(item: ImageCreate, db: Session = Depends(get_db)):
    if db.query(Image).filter(Image.source_file_id == item.source_file_id).first():
        return {"status": "exists", "id": item.source_file_id}

    # Convert list/dict to JSON string for storage
    data = item.dict()
    data['detected_objects'] = json.dumps(item.detected_objects)
    data['image_metadata'] = json.dumps(item.image_metadata)

    new_obj = Image(**data)
    db.add(new_obj)
    db.commit()
    logger.info(f"Loaded Image {item.source_file_id}")
    return {"status": "success"}

if __name__ == "__main__":
    import uvicorn
    host = CONFIG["api"].get("host", "127.0.0.1")
    port = CONFIG["api"].get("port", 8000)
    uvicorn.run(app, host=host, port=port)
