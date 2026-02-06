from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from manager import Source, CONFIG

# Connect to pipeline.db
engine = create_engine(CONFIG["database"]["pipeline_db_url"])
Session = sessionmaker(bind=engine)
session = Session()

# Add a test source (e.g., Hacker News RSS)
this_url = "https://news.ycombinator.com/rss"
if not session.query(Source).filter_by(url=this_url).first():
    new_source = Source(
        url=this_url,
        source_type="rss",
        schedule="*/30 * * * *"  # Run every 30 minutes
    )
    session.add(new_source)
    session.commit()
    print(f"Added source: {new_source.url}")
else:
    print(f"check this: {this_url}")
