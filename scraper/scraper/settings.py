BOT_NAME = 'scraper'

SPIDER_MODULES = ['scraper.spiders']
NEWSPIDER_MODULE = 'scraper.spiders'

ROBOTSTXT_OBEY = False
CONCURRENT_REQUESTS = 16
DOWNLOAD_DELAY = 0.5
CONCURRENT_REQUESTS_PER_DOMAIN = 8
CONCURRENT_REQUESTS_PER_IP = 8

DOWNLOADER_MIDDLEWARES = {
    'scraper.middlewares.RandomUserAgentMiddleware': 400,
    'scraper.middlewares.RandomDelayMiddleware': 410,
}

ITEM_PIPELINES = {
    'scraper.pipelines.LanguageDetectPipeline': 200,
    'scraper.pipelines.MongoDBPipeline': 300,
}

RANDOM_DELAY = 1

MONGO_URI = "mongodb://root:example@crawler_mongodb:27017"
MONGO_DATABASE = 'crawler_db'

FEED_EXPORT_ENCODING = 'utf-8'
LOG_LEVEL = 'DEBUG'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

# Retry settings
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]

# Timeout settings
DOWNLOAD_TIMEOUT = 30
COOKIES_ENABLED = False 