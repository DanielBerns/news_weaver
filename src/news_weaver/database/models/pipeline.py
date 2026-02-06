from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Source(Base):
    __tablename__ = 'sources'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    url = Column(String, nullable=False)

    scraped_files = relationship('ScrapedFile', back_populates='source')

class ScrapedFile(Base):
    __tablename__ = 'scraped_files'

    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey('sources.id'), nullable=False)
    content = Column(String)
    created_at = Column(String)  # Consider using a DateTime type for actual timestamps

    source = relationship('Source', back_populates='scraped_files')
