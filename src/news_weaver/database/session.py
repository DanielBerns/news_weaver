import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

# Configuration for the database
DATABASE_URL = "sqlite:///example.db"  # Update with your database URL

# Create an engine instance
engine = create_engine(DATABASE_URL)

# Create a configured "Session" class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a session instance

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()