from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from manager import Source, Base, CONFIG

engine = create_engine(CONFIG["database"]["pipeline_db_url"])
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Add a test source
if not session.query(Source).filter_by(url="http://example.com").first():
    new_source = Source(url="http://example.com", source_type="website", schedule="*/10 * * * *")
    session.add(new_source)
    session.commit()
    print("Test source added.")
else:
    print("Test source already exists.")
session.close()
