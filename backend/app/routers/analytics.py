from fastapi import APIRouter, Depends
from typing import List, Dict
from datetime import datetime, timedelta
from app.database import get_db
from pymongo.database import Database
import pandas as pd

router = APIRouter()

@router.get("/news-by-date")
async def get_news_by_date(db: Database = Depends(get_db)):
    """Get news count by date for the last 30 days"""
    thirty_days_ago = datetime.now() - timedelta(days=30)
    
    # Aggregate news count by date
    pipeline = [
        {
            "$match": {
                "crawled_at": {"$gte": thirty_days_ago}
            }
        },
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$crawled_at"}},
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]
    
    news_by_date = list(db.news.aggregate(pipeline))
    return [{"date": item["_id"], "count": item["count"]} for item in news_by_date]

@router.get("/news-by-domain")
async def get_news_by_domain(db: Database = Depends(get_db)):
    """Get news count by domain"""
    pipeline = [
        {
            "$group": {
                "_id": "$domain",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ]
    
    news_by_domain = list(db.news.aggregate(pipeline))
    return [{"domain": item["_id"], "count": item["count"]} for item in news_by_domain]

@router.get("/news-by-language")
async def get_news_by_language(db: Database = Depends(get_db)):
    """Get news count by language"""
    pipeline = [
        {
            "$group": {
                "_id": "$meta.language",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ]
    
    news_by_language = list(db.news.aggregate(pipeline))
    return [{"language": item["_id"], "count": item["count"]} for item in news_by_language]

@router.get("/news-trends")
async def get_news_trends(db: Database = Depends(get_db)):
    """Get news trends over time with language breakdown"""
    thirty_days_ago = datetime.now() - timedelta(days=30)
    
    pipeline = [
        {
            "$match": {
                "crawled_at": {"$gte": thirty_days_ago}
            }
        },
        {
            "$group": {
                "_id": {
                    "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$crawled_at"}},
                    "language": "$meta.language"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"_id.date": 1}
        }
    ]
    
    news_trends = list(db.news.aggregate(pipeline))
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame([
        {
            "date": item["_id"]["date"],
            "language": item["_id"]["language"],
            "count": item["count"]
        }
        for item in news_trends
    ])
    
    if not df.empty:
        # Pivot the data
        pivot_df = df.pivot(index='date', columns='language', values='count').fillna(0)
        # Convert to dict format
        return {
            "dates": pivot_df.index.tolist(),
            "languages": pivot_df.columns.tolist(),
            "counts": pivot_df.values.tolist()
        }
    return {"dates": [], "languages": [], "counts": []} 