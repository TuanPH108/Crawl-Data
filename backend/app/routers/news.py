from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Optional
from datetime import datetime
from pymongo.database import Database
from app.database import get_db
import logging

# Cấu hình logging
logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/")
async def get_news(
    db: Database = Depends(get_db),
    skip: int = 0,
    limit: int = 10,
    domain: Optional[str] = None,
    language: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
):
    """Get news articles with optional filtering"""
    try:
        query = {}
        
        if domain:
            query["domain"] = domain
        if language:
            query["meta.language"] = language
        if start_date or end_date:
            query["crawled_at"] = {}
            if start_date:
                query["crawled_at"]["$gte"] = start_date
            if end_date:
                query["crawled_at"]["$lte"] = end_date

        news = list(db.news.find(query).skip(skip).limit(limit))
        total = db.news.count_documents(query)
        
        # Chuyển đổi ObjectId sang chuỗi nếu có
        for item in news:
            if '_id' in item:
                item['_id'] = str(item['_id'])
        
        logger.info(f"Fetched {len(news)} news documents. Total: {total}")

        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "data": news
        }
    except Exception as e:
        logger.error(f"Error fetching news documents: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to fetch news documents: {e}")

@router.get("/{news_id}")
async def get_news_by_id(news_id: str, db: Database = Depends(get_db)):
    """Get a specific news article by ID"""
    try:
        from bson import ObjectId # Import ObjectId ở đây
        if not ObjectId.is_valid(news_id):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid news ID format")

        news = db.news.find_one({"_id": ObjectId(news_id)})
        if not news:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="News article not found")
        
        # Chuyển đổi ObjectId sang chuỗi
        news['_id'] = str(news['_id'])

        logger.info(f"Fetched news document by ID: {news_id}")
        return news
    except Exception as e:
        logger.error(f"Error fetching news document by ID {news_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to fetch news document: {e}")

@router.get("/domain/{domain}")
async def get_news_by_domain(
    domain: str,
    db: Database = Depends(get_db),
    skip: int = 0,
    limit: int = 10
):
    """Get news articles from a specific domain"""
    try:
        query = {"domain": domain}
        news = list(db.news.find(query).skip(skip).limit(limit))
        total = db.news.count_documents(query)

        # Chuyển đổi ObjectId sang chuỗi
        for item in news:
            if '_id' in item:
                item['_id'] = str(item['_id'])
        
        logger.info(f"Fetched {len(news)} news documents for domain '{domain}'. Total: {total}")

        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "data": news
        }
    except Exception as e:
        logger.error(f"Error fetching news documents by domain {domain}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to fetch news documents by domain: {e}")

@router.get("/language/{language}")
async def get_news_by_language(
    language: str,
    db: Database = Depends(get_db),
    skip: int = 0,
    limit: int = 10
):
    """Get news articles in a specific language"""
    try:
        query = {"meta.language": language}
        news = list(db.news.find(query).skip(skip).limit(limit))
        total = db.news.count_documents(query)

        # Chuyển đổi ObjectId sang chuỗi
        for item in news:
            if '_id' in item:
                item['_id'] = str(item['_id'])
        
        logger.info(f"Fetched {len(news)} news documents for language '{language}'. Total: {total}")

        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "data": news
        }
    except Exception as e:
        logger.error(f"Error fetching news documents by language {language}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to fetch news documents by language: {e}")

@router.get("/search/{keyword}")
async def search_news(
    keyword: str,
    db: Database = Depends(get_db),
    skip: int = 0,
    limit: int = 10
):
    """Search news articles by keyword in title or content"""
    try:
        query = {
            "$or": [
                {"title": {"$regex": keyword, "$options": "i"}},
                {"content": {"$regex": keyword, "$options": "i"}}
            ]
        }
        
        news = list(db.news.find(query).skip(skip).limit(limit))
        total = db.news.count_documents(query)

        # Chuyển đổi ObjectId sang chuỗi
        for item in news:
            if '_id' in item:
                item['_id'] = str(item['_id'])
        
        logger.info(f"Fetched {len(news)} news documents for search keyword '{keyword}'. Total: {total}")

        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "data": news
        }
    except Exception as e:
        logger.error(f"Error searching news documents with keyword {keyword}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to search news documents: {e}") 