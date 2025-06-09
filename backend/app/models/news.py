from sqlalchemy import Column, Integer, String, DateTime, JSON
from app.database import Base

class News(Base):
    __tablename__ = "news"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(String, unique=True, index=True)
    title = Column(String)
    content = Column(String)
    domain = Column(String, index=True)
    crawled_at = Column(DateTime)
    meta = Column(JSON)  # For storing additional metadata like language 