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
        # Get text for language detection
        text = (item.get('title', '') + ' ' + item.get('content', ''))[:1000]
        if not text.strip():
            pipeline_logger.warning(f"Empty text for language detection - URL: {item.get('url')}")
            raise DropItem("Empty text for language detection")

        try:
            # First check if language is already set and valid
            if item.get('language') not in ['zh', 'vi']:
                pipeline_logger.info(f"Invalid language code in item: {item.get('language')} - URL: {item.get('url')}")
                raise DropItem(f"Invalid language code: {item.get('language')}")

            # Perform language detection
            detected_lang = detect(text)
            pipeline_logger.info(f"[Language Detection] URL: {item.get('url')} - Detected: {detected_lang}, Item language: {item.get('language')}")

            # Map detected language to expected format
            if detected_lang in ['zh-cn', 'zh-tw', 'zh']:
                detected_lang = 'zh'
                pipeline_logger.info(f"[Language Mapping] URL: {item.get('url')} - Mapped {detected_lang} to zh")
            elif detected_lang == 'vi':
                detected_lang = 'vi'
            else:
                pipeline_logger.info(f"[Language Rejection] URL: {item.get('url')} - Skipping non-zh/vi content: {detected_lang}")
                raise DropItem(f"Non-zh/vi content detected: {detected_lang}")

            # Verify language matches item's language
            if detected_lang != item.get('language'):
                pipeline_logger.warning(
                    f"[Language Mismatch] URL: {item.get('url')} - "
                    f"Item language: {item.get('language')}, Detected: {detected_lang}"
                )
                raise DropItem(f"Language mismatch: item={item.get('language')}, detected={detected_lang}")

            # Update item with detected language
            item['language'] = detected_lang
            item['meta'] = item.get('meta', {})
            item['meta']['detected_language'] = detected_lang
            item['meta']['language_detection_confidence'] = 'high'

            pipeline_logger.info(f"[Language Detection Success] URL: {item.get('url')} - Final language: {detected_lang}")
            return item

        except Exception as e:
            if isinstance(e, DropItem):
                raise
            error_info = {
                'url': item.get('url', 'unknown'),
                'error_type': 'language_detection_error',
                'error_message': str(e),
                'detected_language': detected_lang if 'detected_lang' in locals() else 'unknown',
                'item_language': item.get('language', 'unknown'),
                'timestamp': datetime.utcnow().isoformat()
            }
            pipeline_logger.error(json.dumps(error_info, ensure_ascii=False))
            raise DropItem(f"Language detection failed: {str(e)}")

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
        try:
            # Strict validation before saving to MongoDB
            if not item.get('language') in ['zh', 'vi']:
                self.logger.warning(
                    f"[MongoDB Rejection] URL: {item.get('url')} - "
                    f"Invalid language: {item.get('language')}"
                )
                raise DropItem(f"Invalid language for MongoDB: {item.get('language')}")

            # Verify language detection confidence
            if item.get('meta', {}).get('language_detection_confidence') != 'high':
                self.logger.warning(
                    f"[MongoDB Rejection] URL: {item.get('url')} - "
                    f"Low language detection confidence"
                )
                raise DropItem("Low language detection confidence")

            # Check required fields
            required_fields = ['url', 'title', 'content', 'language', 'domain']
            missing_fields = [field for field in required_fields if not item.get(field)]
            if missing_fields:
                error_msg = f"Missing required fields: {', '.join(missing_fields)}"
                self.logger.error(json.dumps({
                    'error_type': 'missing_required_fields',
                    'error_message': error_msg,
                    'url': item.get('url', 'unknown'),
                    'missing_fields': missing_fields,
                    'timestamp': datetime.utcnow().isoformat()
                }, ensure_ascii=False))
                raise DropItem(error_msg)

            # Add timestamp
            item['crawled_at'] = datetime.utcnow()
            
            try:
                # Add new with validation
                result = self.collection.insert_one(dict(item))
                self.logger.info(
                    f"[MongoDB Success] URL: {item['url']} - "
                    f"Language: {item['language']} - "
                    f"ObjectId: {result.inserted_id} - "
                    f"Collection: {self.collection.name}"
                )
            except DuplicateKeyError:
                # Update if URL exists
                result = self.collection.update_one(
                    {'url': item['url']},
                    {'$set': dict(item)}
                )
                # Get the document to log its _id
                doc = self.collection.find_one({'url': item['url']})
                self.logger.info(
                    f"[MongoDB Update] URL: {item['url']} - "
                    f"Language: {item['language']} - "
                    f"ObjectId: {doc['_id'] if doc else 'unknown'} - "
                    f"Collection: {self.collection.name}"
                )
            
            return item
            
        except OperationFailure as e:
            error_msg = f"MongoDB operation failed: {str(e)}"
            self.logger.error(json.dumps({
                'error_type': 'mongodb_operation_error',
                'error_message': error_msg,
                'url': item.get('url', 'unknown'),
                'title': item.get('title', 'unknown'),
                'domain': item.get('domain', 'unknown'),
                'language': item.get('language', 'unknown'),
                'collection': self.collection.name,
                'timestamp': datetime.utcnow().isoformat()
            }, ensure_ascii=False))
            raise DropItem(error_msg)
        except Exception as e:
            error_msg = f"Unknown error in process_item: {str(e)}"
            self.logger.error(json.dumps({
                'error_type': 'unknown_error',
                'error_message': error_msg,
                'stack_trace': traceback.format_exc(),
                'url': item.get('url', 'unknown'),
                'title': item.get('title', 'unknown'),
                'domain': item.get('domain', 'unknown'),
                'language': item.get('language', 'unknown'),
                'collection': self.collection.name,
                'timestamp': datetime.utcnow().isoformat()
            }, ensure_ascii=False))
            raise DropItem(error_msg) 