import scrapy
import random
import re
from urllib.parse import urlparse
from scrapy.exceptions import CloseSpider
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from langdetect import detect, LangDetectException
import sys
import os
import time

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure root logger for spider
spider_handler = logging.FileHandler('logs/spider.log')
spider_error_handler = logging.FileHandler('logs/spider_errors.log')
spider_error_handler.setLevel(logging.ERROR)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Console output
        spider_handler,  # Spider log file
        spider_error_handler  # Spider error log file
    ]
)
logger = logging.getLogger(__name__)

class NewsSpider(scrapy.Spider):
    name = "news_spider"
    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'DEPTH_LIMIT': 10,
        'DEPTH_PRIORITY': 2,
        'DEPTH_STATS': True,
        'DEPTH_STATS_VERBOSE': True,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 10,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429, 403, 111],
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'LOG_LEVEL': 'WARNING',
        'LOG_STDOUT': False,     # Không log ra stdout
        'LOG_FILE': 'logs/spider.log',  # Log vào file
        'COOKIES_ENABLED': True,
        'DOWNLOAD_TIMEOUT': 300,  # Increased from 180 to 300 seconds
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,
        'REDIRECT_ENABLED': True,
        'REDIRECT_MAX_TIMES': 5,
        'REDIRECT_PRIORITY_ADJUST': 1,
        'CLOSESPIDER_TIMEOUT': 14400,  # Increased from 7200 to 14400 seconds (4 hours)
        'CLOSESPIDER_ITEMCOUNT': 100000,
        'CLOSESPIDER_PAGECOUNT': 100000,
        'CLOSESPIDER_ERRORCOUNT': 100,
        'MEMUSAGE_ENABLED': True,
        'MEMUSAGE_LIMIT_MB': 2048,
        'MEMUSAGE_WARNING_MB': 768,
        'REACTOR_THREADPOOL_MAXSIZE': 8,
        'DNS_TIMEOUT': 30,
        'DOWNLOAD_MAXSIZE': 10485760,
        'DOWNLOAD_WARNSIZE': 5242880,
        'AJAXCRAWL_ENABLED': True,
        'ROBOTSTXT_OBEY': True,
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_EXPIRATION_SECS': 3600,
        'HTTPCACHE_DIR': 'httpcache',
        'HTTPCACHE_IGNORE_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429, 403],
        'HTTPCACHE_STORAGE': 'scrapy.extensions.httpcache.FilesystemCacheStorage'
    }

    selector_map = {
        # Vietnamese domains
        'nhandan.vn': {
            'title': 'h1.article__title cms-title, h1.title, h1',
            'content': 'div.article__body.zce-content-body.cms-body p, div.sda_middle',
            'date': 'span.date, div.time, div.date, time'
        },
        'qdnd.vn': {
            'title': 'h1.title, h1.post-title, h1',
            'content': 'div.articleBody p',
            'date': 'span.time, div.date, div.time, span.post-subinfo span'
        },
        'vietnamplus.vn': {
            'title': 'h1.title, h1, h1.article__title.cms-title ',
            'content': 'div.article__body.zce-content-body.cms-body p',
            'date': 'span.datetime, div.time, div.date, time'
        },
        'vovworld.vn/vi-VN.vov': {
            'title': 'h1.title, h1, h1.article-title.mb-0',
            'content': 'div.text-long p',
            'date': 'span.time, div.date, div.time, div.col-md-4.mb-2'
        },
        'thoidai.com.vn': {
            'title': 'h1.title, h1, h1.article-detail-title.f0',
            'content': 'div.__MASTERCMS_CONTENT.fw.f1.mb20.clearfix p',
            'date': 'span.article-detail-date.rt'
        },
        'sggp.org.vn': {
            'title': 'h1.title, h1, h1.article__title.cms-title',
            'content': 'div.article__body.zce-content-body.cms-body p',
            'date': 'span.time'
        },
        'vnanet.vn/vi': {
            'title': 'h1.title-detail, h1.title',
            'content': 'article.fck_detail, div#main_detail, div#content_detail',
            'date': 'span.date, div.time, div.date'
        },
        'baochinhphu.vn': {
            'title': 'h1.title, h1, h1.detail-title',
            'content': 'div.detail-content.afcbc-body.clearfix p, div.detail-content.afcbc-body.clearfix p span',
            'date': 'div.publishDate'
        },
        'cand.com.vn': {
            'title': 'h1.title, h1, h1.box-title-detail.entry-title',
            'content': 'div.detail-content-body p',
            'date': 'div.box-date'
        },

        # Chinese domains
        'cn.nhandan.vn': {
            'title': 'h1.article__title cms-title, h1.title, h1',
            'content': 'div.article__body.zce-content-body.cms-body p, div.sda_middle, p.t1, p[class*="t1"]',
            'date': 'span.date, div.time, div.date, time'
        },
        'cn.qdnd.vn': {
            'title': 'h1.title, h1.post-title, h1',
            'content': 'div[itemprop="articleBody"] p',
            'date': 'span.time, div.date, div.time, span.post-subinfo span'
        },
        'zh.vietnamplus.vn': {
            'title': 'h1.title, h1, h1.article__title.cms-title ',
            'content': 'div.article__body.zce-content-body.cms-body p',
            'date': 'span.datetime, div.time, div.date, time'
        },
        'vovworld.vn/zh-CN.vov': {
            'title': 'h1, h1.title, h1, h1.article-title.mb-0',
            'content': 'div.article__body.cms-body div, div.article__body.cms-body p',
            'date': 'span.time, div.date, div.time, div.col-md-4.mb-2'
        },
        'shidai.thoidai.com.vn': {
            'title': 'h1.title, h1, h1.article-detail-title.f0',
            'content': 'div.article-detail-content.fw.lt.clearfix p',
            'date': 'span.article-detail-date.rt'
        },
        'cn.sggp.org.vn': {
            'title': 'h1.title, h1, h1.article__title.cms-title',
            'content': 'div.article__body.zce-content-body.cms-body p',
            'date': 'span.time'
        },
        'vnanet.vn/zh': {
            'title': 'h1.title-detail, h1.title',
            'content': 'article.fck_detail, div#main_detail, div#content_detail, div.article-content',
            'date': 'div.publishDate'
        },
        'cn.baochinhphu.vn': {
            'title': 'h1.title, h1, h1.detail-title',
            'content': 'div.detail-content.afcbc-body.clearfix p, div.detail-content.afcbc-body.clearfix p span',
            'date': 'div.publishDate'
        },
        'cn.cand.com.vn': {
            'title': 'h1.title, h1, h1.box-title-detail.entry-title, div.box-des',
            'content': 'div.detail-content-body p, div.box-article p, div.box-article p',
            'date': 'div.box-date'
        }
    }

    def __init__(self, urls=None, timeout=60, task_id=None, collection_name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.request_timeout = int(timeout)
        self.task_id = task_id
        self.crawled_urls = set()
        self.collection_name = collection_name
        self.failed_urls = set()
        
        if not urls:
            logger.error("No URLs provided")
            raise CloseSpider("No URLs provided")
            
        if isinstance(urls, str):
            self.start_urls = urls.split(",")
        elif isinstance(urls, list):
            self.start_urls = urls
        else:
            logger.error(f"Invalid URLs type: {type(urls)}")
            raise CloseSpider("Invalid URLs type")
            
        self.allowed_domains = []
        
        for url in self.start_urls:
            try:
                parsed = urlparse(url)
                if parsed.scheme not in ['http', 'https']:
                    logger.error(f"Invalid URL scheme: {url}")
                    continue
                domain = parsed.netloc.replace('www.', '')
                if domain:
                    self.allowed_domains.append(domain)
                else:
                    logger.error(f"Invalid domain for URL: {url}")
            except Exception as e:
                logger.error(f"Error parsing URL {url}: {str(e)}")
                
        if not self.allowed_domains:
            logger.error("No valid domains found in URLs")
            raise CloseSpider("No valid domains found in URLs")
            
        logger.info(f"Starting spider with domains: {self.allowed_domains}")

    def start_requests(self):
        for url in self.start_urls:
            if url not in self.crawled_urls:
                logger.info(f"Starting crawl: {url}")
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    errback=self.handle_error,
                    meta={
                        'download_timeout': self.request_timeout,
                        'dont_redirect': True,
                        'handle_httpstatus_list': [301, 302, 403, 404, 500],
                        'dont_merge_cookies': False
                    },
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'vi,en-US;q=0.7,en;q=0.3',
                        'Cache-Control': 'no-cache',
                        'Pragma': 'no-cache',
                        'Referer': 'https://www.google.com/'
                    },
                    dont_filter=True
                )

    def handle_error(self, failure):
        url = failure.request.url
        self.failed_urls.add(url)
        
        if failure.check(HttpError):
            # Xử lý HTTP error
            if failure.value.response.status in [403, 429]:
                # Rate limit, sleep longer
                time.sleep(random.uniform(5, 10))
        elif failure.check(TimeoutError):
            # Tăng timeout cho retry
            failure.request.meta['download_timeout'] *= 2

    def parse(self, response):
        try:
            # Thêm timeout cho mỗi request
            timeout = 30
            start_time = time.time()
            
            self.crawled_urls.add(response.url)
            
            # Kiểm tra depth trước khi xử lý
            current_depth = response.meta.get('depth', 0)
            if current_depth > self.custom_settings['DEPTH_LIMIT']:
                logger.info(f"Reached depth limit for URL: {response.url}")
                return
            
            # Thử parse bài viết trước
            yield from self.parse_article(response)
            
            # Lấy tất cả link và crawl
            links = response.css('a::attr(href)').getall()
            
            # Giới hạn số lượng link crawl từ mỗi trang
            max_links_per_page = 100
            links = links[:max_links_per_page]
            
            for link in links:
                try:
                    absolute_url = response.urljoin(link)
                    if absolute_url in self.crawled_urls:
                        continue
                        
                    parsed_url = urlparse(absolute_url)
                    if parsed_url.scheme not in ['http', 'https']:
                        continue
                        
                    if parsed_url.hostname and parsed_url.hostname.replace('www.', '') not in self.allowed_domains:
                        continue
                    
                    # Kiểm tra depth của URL
                    if current_depth >= self.custom_settings['DEPTH_LIMIT']:
                        continue

                    # Ưu tiên thử crawl như bài viết trước
                    yield scrapy.Request(
                        url=absolute_url,
                        callback=self.parse_article,
                        errback=self.handle_error,
                        meta={
                            'download_timeout': self.custom_settings['DOWNLOAD_TIMEOUT'],
                            'dont_redirect': False,
                            'handle_httpstatus_list': [301, 302, 403, 404, 500],
                            'dont_merge_cookies': False,
                            'depth': current_depth + 1,
                            'download_slot': parsed_url.hostname
                        },
                        headers={
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                            'Accept-Language': 'vi,en-US;q=0.7,en;q=0.3',
                            'Cache-Control': 'no-cache',
                            'Pragma': 'no-cache',
                            'Referer': response.url
                        },
                        dont_filter=True,
                        priority=3  # Ưu tiên cao hơn
                    )

                    # Sau đó tiếp tục crawl như trang thông thường
                    yield scrapy.Request(
                        url=absolute_url,
                        callback=self.parse,
                        errback=self.handle_error,
                        meta={
                            'download_timeout': self.custom_settings['DOWNLOAD_TIMEOUT'],
                            'dont_redirect': False,
                            'handle_httpstatus_list': [301, 302, 403, 404, 500],
                            'dont_merge_cookies': False,
                            'depth': current_depth + 1,
                            'download_slot': parsed_url.hostname
                        },
                        headers={
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                            'Accept-Language': 'vi,en-US;q=0.7,en;q=0.3',
                            'Cache-Control': 'no-cache',
                            'Pragma': 'no-cache',
                            'Referer': response.url
                        },
                        dont_filter=True,
                        priority=1
                    )
                except Exception as e:
                    logger.error(f"Error processing link {link}: {str(e)}")
                    continue

            if time.time() - start_time > timeout:
                logger.warning(f"Parse timeout for {response.url}")
                return
            
        except Exception as e:
            logger.error(f"Parse error for {response.url}: {str(e)}")
        finally:
            # Cleanup
            response = None

    def clean_text(self, text):
        if not text:
            return ""
            
        # Loại bỏ các ký tự xuống dòng không mong muốn
        text = re.sub(r'\n+', ' ', text)
        
        # Loại bỏ các khoảng trắng thừa
        text = re.sub(r'\s+', ' ', text)
        
        # Loại bỏ các ký tự đặc biệt không mong muốn như \n, \r, \t
        text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        
        # Loại bỏ các ký tự điều khiển
        text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\r\t')
        
        # Chuẩn hóa khoảng trắng
        text = ' '.join(text.split())
        
        return text.strip()

    def extract_meta_content(self, response, meta_name):
        return response.xpath(f"//meta[@name='{meta_name}']/@content").get() or \
               response.xpath(f"//meta[@property='{meta_name}']/@content").get() or ""

    def normalize_domain(self, url):
        """
        Normalize domain from URL and determine language code
        Returns tuple of (domain, language_code)
        """
        try:
            parsed = urlparse(url)
            hostname = parsed.netloc.lower().replace('www.', '')
            
            # Remove special characters from start and end
            hostname = re.sub(r'^[^a-z0-9]+|[^a-z0-9]+$', '', hostname)
            
            # Handle special cases
            if 'vovworld.vn' in hostname:
                if '/zh-CN.vov' or 'zh-CN'in url:
                    return 'vovworld.vn', 'zh'
                elif '/vi-VN.vov' or 'vi-VN' in url:
                    return 'vovworld.vn', 'vi'
                    
            if 'vnanet.vn' in hostname:
                if '/zh/' in url:
                    return 'vnanet.vn', 'zh'
                elif '/vi/' in url:
                    return 'vnanet.vn', 'vi'
                    
            if 'thoidai.com.vn' in hostname:
                if 'shidai.' in hostname:
                    return 'thoidai.com.vn', 'zh'
                else:
                    return 'thoidai.com.vn', 'vi'
            
            # Handle Chinese domains
            chinese_domains = {
                'cn.nhandan.vn': 'nhandan.vn',
                'cn.qdnd.vn': 'qdnd.vn',
                'zh.vietnamplus.vn': 'vietnamplus.vn',
                'cn.sggp.org.vn': 'sggp.org.vn',
                'cn.baochinhphu.vn': 'baochinhphu.vn',
                'cn.cand.com.vn': 'cand.com.vn'
            }
            
            if hostname in chinese_domains:
                return chinese_domains[hostname], 'zh'
            elif hostname.startswith('cn.'):
                return hostname[3:], 'zh'
            elif hostname.startswith('zh.'):
                return hostname[3:], 'zh'
            else:
                return hostname, 'vi'
                
        except Exception as e:
            logger.error(f"Error normalizing domain for {url}: {str(e)}")
            return None, None

    def get_selector_key(self, domain, language_code):
        """
        Get the appropriate selector key based on domain and language code
        """
        if not domain:
            return None
            
        # Handle special cases
        if domain == 'vovworld.vn':
            return f"{domain}/{language_code}-{'CN' if language_code == 'zh' else 'VN'}.vov"
        elif domain == 'vnanet.vn':
            return f"{domain}/{language_code}"
        elif domain == 'thoidai.com.vn' and language_code == 'zh':
            return f"shidai.{domain}"
            
        # Handle Chinese domains
        chinese_domains = {
            'nhandan.vn': 'cn.nhandan.vn',
            'qdnd.vn': 'cn.qdnd.vn',
            'vietnamplus.vn': 'zh.vietnamplus.vn',
            'sggp.org.vn': 'cn.sggp.org.vn',
            'baochinhphu.vn': 'cn.baochinhphu.vn',
            'cand.com.vn': 'cn.cand.com.vn'
        }
        
        if language_code == 'zh':
            return chinese_domains.get(domain, f"cn.{domain}")
        return domain

    def parse_article(self, response):
        # Normalize domain and get language code
        domain, language_code = self.normalize_domain(response.url)
        if not domain:
            logger.error(f"Could not normalize domain for URL: {response.url}")
            return
            
        # Get selector key and log for debugging
        selector_key = self.get_selector_key(domain, language_code)
        logger.info(f"Processing URL: {response.url}")
        logger.info(f"Normalized domain: {domain}, Language code: {language_code}")
        logger.info(f"Selector key: {selector_key}")
        
        selectors = self.selector_map.get(selector_key, {})
        if not selectors:
            logger.warning(f"No selectors found for key: {selector_key}")
            return
            
        logger.info(f"Found selectors: {selectors}")
        
        # Lấy title
        title = (
            response.css(selectors.get('title', 'h1')).xpath('string()').get() or
            self.extract_meta_content(response, 'og:title') or
            self.extract_meta_content(response, 'title') or
            ""
        )
        title = self.clean_text(title)
        
        # Lấy content
        content = ""
        try:
            content_selector = selectors.get('content')
            if not content_selector:
                logger.warning(f"No content selector found for {selector_key}")
                return
                
            content_blocks = response.css(content_selector)
            if not content_blocks:
                logger.warning(f"No content blocks found for selector: {content_selector}")
                return

            raw_text_nodes = content_blocks.xpath(
                """
                    .//text()[
                        not(ancestor::style)
                        and not(ancestor::script)
                        and not(ancestor::div[contains(@class, 'article__tag')])
                        and not(ancestor::*[contains(@class, 'social-pin')])
                        and not(ancestor::*[contains(@class, 'sticky')])
                        and not(ancestor::*[contains(@class, 'article__social')])
                    ]
                """
            ).getall()

            paragraphs = [re.sub(r'\s+', ' ', t.strip()) for t in raw_text_nodes if t.strip()]
            content = "\n".join(paragraphs)
            
            if not content:
                logger.warning(f"No content extracted for URL: {response.url}")
                return
                
        except Exception as e:
            logger.error(f"Error extracting content for {response.url}: {str(e)}")
            return

        # Kiểm tra ngôn ngữ dựa trên cả title và content
        try:
            text_to_check = (title + " " + content)[:1000]
            if not text_to_check.strip():
                logger.warning(f"No text to check language for URL: {response.url}")
                return
                
            detected_lang = detect(text_to_check)
            logger.info(f"Detected language: {detected_lang}, Expected: {language_code}")
            
            # Map detected language to expected format
            if detected_lang in ['zh-cn', 'zh-tw']:
                detected_lang = 'zh'
                
            if detected_lang != language_code:
                logger.warning(f"Language mismatch for {response.url}: detected {detected_lang}, expected {language_code}")
                return
                
        except LangDetectException as e:
            logger.error(f"Language detection failed for {response.url}: {str(e)}")
            return
        
        # Lấy date
        date_selector = selectors.get('date', 'span.date, div.time, div.date, time')
        date = None
        
        # Thử lấy date từ các selector trong selector_map
        if date_selector:
            date = response.css(date_selector).xpath('string()').get()
            
        # Nếu không tìm thấy, thử các selector phổ biến khác
        if not date:
            common_selectors = [
                'span[class*="date"]',
                'span[class*="time"]',
                'div[class*="date"]',
                'div[class*="time"]',
                'time[datetime]',
                'meta[property="article:published_time"]',
                'meta[name="pubdate"]'
            ]
            for selector in common_selectors:
                date = response.css(selector).xpath('string()').get()
                if date:
                    break
                    
        # Nếu vẫn không tìm thấy, thử lấy từ meta tags
        if not date:
            date = (
                self.extract_meta_content(response, 'article:published_time') or
                self.extract_meta_content(response, 'pubdate') or
                ""
            )
            
        date = self.clean_text(date)
        
        # Tạo item
        item = {
            'url': response.url,
            'title': title,
            'content': content,
            'date': date,
            'domain': domain,
            'language': language_code,
            'crawled_at': datetime.now().isoformat(),
            'task_id': self.task_id,
            'meta': {
                'description': self.extract_meta_content(response, 'description'),
                'keywords': self.extract_meta_content(response, 'keywords'),
                'language': self.extract_meta_content(response, 'language'),
                'robots': self.extract_meta_content(response, 'robots'),
                'detected_language': detected_lang
            }
        }
        
        logger.info(f"Crawl successful: {response.url} - Language: {language_code}")
        yield item

    def closed(self, reason):
        logger.info(f"Spider finished: {reason}") 