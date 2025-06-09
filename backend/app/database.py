from pymongo import MongoClient
from pymongo.database import Database
import os
from dotenv import load_dotenv

load_dotenv()

# Get MongoDB URI from environment variable
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://root:example@mongodb:27017/")

# Create MongoDB client
client = MongoClient(MONGODB_URI)

# Get database
db = client.crawler_db

# Get collections
raw_vietnamese_news = db.auto_crawl_vi
raw_chinese_news = db.auto_craw_zh

# Dependency to get database
def get_db() -> Database:
    try:
        yield db
    finally:
        pass  # MongoDB client handles connection pooling automatically 