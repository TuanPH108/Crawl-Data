from langdetect import detect
from pymongo import MongoClient
from datetime import datetime
import os
from scrapy.exceptions import DropItem
from pymongo.errors import DuplicateKeyError, ConnectionFailure, OperationFailure
import logging
import json
import traceback
import sys

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure root logger for pipeline
pipeline_handler = logging.FileHandler('logs/pipeline.log')
pipeline_error_handler = logging.FileHandler('logs/pipeline_errors.log')
pipeline_error_handler.setLevel(logging.ERROR)

# Create pipeline logger
pipeline_logger = logging.getLogger('pipeline')
pipeline_logger.setLevel(logging.INFO)
pipeline_logger.addHandler(pipeline_handler)
pipeline_logger.addHandler(pipeline_error_handler)
pipeline_logger.addHandler(logging.StreamHandler(sys.stdout))

class LanguageDetectPipeline:
    def process_item(self, item, spider):
        text = (item.get('title', '') + ' ' + item.get('content', ''))[:1000]
        try:
            lang = detect(text)
            if lang == 'vi':
                item['language'] = 'vi'
            elif lang in ['zh-cn', 'zh-tw', 'zh']:
                item['language'] = 'zh'
            else:
                # Skip non-zh/vi content
                pipeline_logger.info(f"Skipping non-zh/vi content for URL: {item.get('url')} - Detected language: {lang}")
                raise DropItem(f"Non-zh/vi content detected: {lang}")
        except Exception as e:
            if isinstance(e, DropItem):
                raise
            pipeline_logger.error(json.dumps({
                'url': item.get('url', 'unknown'),
                'error_type': 'lỗi_phát_hiện_ngôn_ngữ',
                'error_message': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }, ensure_ascii=False))
            raise DropItem(f"Language detection failed: {str(e)}")
        return item

class MongoDBPipeline:
    def __init__(self):
        self.logger = pipeline_logger

    def open_spider(self, spider):
        try:
            mongo_uri = 'mongodb://root:example@host.docker.internal:27017/crawler_mongodb?authSource=admin'
            self.logger.info(f"Connecting to MongoDB at {mongo_uri}")
            
            self.client = MongoClient(mongo_uri)
            # Check connection
            self.client.admin.command('ping')
            self.logger.info("MongoDB connection successful")
            
            self.db = self.client['crawler_db']
            # Get collection_name from spider if available, default to spider.name
            collection_name = getattr(spider, 'collection_name', None) or spider.name
            self.collection = self.db[collection_name]
            self.logger.info(f"Using database: crawler_db, collection: {collection_name}")
            
            # Create index
            self.collection.create_index([("crawled_at", -1)])
            self.collection.create_index([("url", 1)], unique=True)
            self.logger.info("Created indexes for crawled_at and url")
            
        except ConnectionFailure as e:
            error_info = {
                'error_type': 'mongodb_connection_error',
                'error_message': str(e),
                'timestamp': datetime.utcnow().isoformat(),
                'mongo_uri': mongo_uri
            }
            pipeline_logger.error(json.dumps(error_info, ensure_ascii=False))
            raise
        except Exception as e:
            error_info = {
                'error_type': 'unknown_error',
                'error_message': str(e),
                'stack_trace': traceback.format_exc(),
                'timestamp': datetime.utcnow().isoformat()
            }
            pipeline_logger.error(json.dumps(error_info, ensure_ascii=False))
            raise

    def close_spider(self, spider):
        try:
            self.client.close()
            self.logger.info("MongoDB connection closed")
        except Exception as e:
            error_info = {
                'error_type': 'mongodb_close_error',
                'error_message': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
            pipeline_logger.error(json.dumps(error_info, ensure_ascii=False))

    def process_item(self, item, spider):
        # Only process items with language 'zh' or 'vi'
        if item.get('language') not in ['zh', 'vi']:
            self.logger.info(f"Skipping document with invalid language: {item.get('language')} for URL: {item.get('url')}")
            raise DropItem(f"Invalid language: {item.get('language')}")

        item['crawl_status'] = 'success'
        item['crawl_time'] = datetime.utcnow()
        try:
            # Check required fields
            required_fields = ['url', 'title', 'content', 'language']
            for field in required_fields:
                if not item.get(field):
                    error_msg = f"Missing required field: {field}"
                    pipeline_logger.error(json.dumps({
                        'error_type': 'missing_field',
                        'error_message': error_msg,
                        'url': item.get('url', 'unknown'),
                        'title': item.get('title', 'unknown'),
                        'domain': item.get('domain', 'unknown'),
                        'language': item.get('language', 'unknown'),
                        'timestamp': datetime.utcnow().isoformat()
                    }, ensure_ascii=False))
                    item['crawl_status'] = 'failed'
                    item['error_message'] = error_msg
                    return item
            
            # Add timestamp
            item['crawled_at'] = datetime.utcnow()
            
            try:
                # Add new with validation
                result = self.collection.insert_one(dict(item))
                pipeline_logger.info(f"Added new document - URL: {item['url']} - Language: {item['language']} - ObjectId: {result.inserted_id}")
            except DuplicateKeyError:
                # Update if URL exists
                result = self.collection.update_one(
                    {'url': item['url']},
                    {'$set': dict(item)}
                )
                # Get the document to log its _id
                doc = self.collection.find_one({'url': item['url']})
                pipeline_logger.info(f"Updated document - URL: {item['url']} - Language: {item['language']} - ObjectId: {doc['_id'] if doc else 'unknown'}")
            
            return item
            
        except OperationFailure as e:
            error_msg = f"MongoDB operation failed: {str(e)}"
            pipeline_logger.error(json.dumps({
                'error_type': 'mongodb_operation_error',
                'error_message': error_msg,
                'url': item.get('url', 'unknown'),
                'title': item.get('title', 'unknown'),
                'domain': item.get('domain', 'unknown'),
                'language': item.get('language', 'unknown'),
                'timestamp': datetime.utcnow().isoformat()
            }, ensure_ascii=False))
            item['crawl_status'] = 'failed'
            item['error_message'] = error_msg
            return item
        except Exception as e:
            error_msg = f"Unknown error in process_item: {str(e)}"
            pipeline_logger.error(json.dumps({
                'error_type': 'unknown_error',
                'error_message': error_msg,
                'stack_trace': traceback.format_exc(),
                'url': item.get('url', 'unknown'),
                'title': item.get('title', 'unknown'),
                'domain': item.get('domain', 'unknown'),
                'language': item.get('language', 'unknown'),
                'timestamp': datetime.utcnow().isoformat()
            }, ensure_ascii=False))
            item['crawl_status'] = 'failed'
            item['error_message'] = error_msg
            return item 