import json
from fastapi import FastAPI, HTTPException, Depends, Header
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from datetime import datetime

# Import shared config
from news_weaver.common.config import CONFIG, setup_logger
from pydantic import BaseModel

logger = setup_logger("Loader")
app = FastAPI(title="ETL Loader API")

# --- Data Database (Distinct from Pipeline DB) ---
DATA_DB_URL = CONFIG["database"]["data_db_url"]
engine = create_engine(DATA_DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# --- Models (Specific to Loader) ---
class Article(Base):
    __tablename__ = "articles"
    id = Column(Integer, primary_key=True)
    source_file_id = Column(Integer, unique=True)
    url = Column(String)
    title = Column(String)
    content = Column(Text)
    ingested_at = Column(DateTime, default=datetime.utcnow)

# (Add Document, Image, Spreadsheet models here similar to original...)

Base.metadata.create_all(bind=engine)

# --- Pydantic & Routes ---
class ArticleCreate(BaseModel):
    source_file_id: int
    url: str
    title: str = None
    content: str

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def verify_key(x_api_key: str = Header(...)):
    if x_api_key != CONFIG["api"]["secret_key"]:
        raise HTTPException(403, "Invalid Key")

@app.post("/articles", dependencies=[Depends(verify_key)])
def create_article(item: ArticleCreate, db: Session = Depends(get_db)):
    if db.query(Article).filter_by(source_file_id=item.source_file_id).first():
        return {"status": "exists"}

    new_article = Article(**item.dict())
    db.add(new_article)
    db.commit()
    logger.info(f"Loaded article {item.source_file_id}")
    return {"status": "success"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=CONFIG["api"]["host"], port=CONFIG["api"]["port"])
