from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from .config import CONFIG

# Pipeline Database (Shared by Manager, Extractor, Transformer)
PIPELINE_DB_URL = CONFIG["database"]["pipeline_db_url"]
pipeline_engine = create_engine(PIPELINE_DB_URL)
PipelineSessionLocal = sessionmaker(bind=pipeline_engine)
PipelineBase = declarative_base()

def get_pipeline_db():
    """Dependency for context managers or FastAPI."""
    db = PipelineSessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_pipeline_db():
    PipelineBase.metadata.create_all(pipeline_engine)
