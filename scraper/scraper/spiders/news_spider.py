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
        'DOWNLOAD_DELAY': 1,
        'DEPTH_LIMIT':6,
        'DEPTH_PRIORITY': 2,
        'DEPTH_STATS': True,
        'DEPTH_STATS_VERBOSE': True,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 10,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429, 403, 111],
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'LOG_LEVEL': 'WARNING',
        'LOG_STDOUT': False, 
        'LOG_FILE': 'logs/spider.log',
        'COOKIES_ENABLED': True,
        'DOWNLOAD_TIMEOUT': 300, 
        'CONCURRENT_REQUESTS': 32,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 16,
        'REDIRECT_ENABLED': True,
        'REDIRECT_MAX_TIMES': 5,
        'REDIRECT_PRIORITY_ADJUST': 1,
        'CLOSESPIDER_TIMEOUT': 36000,
        # 'CLOSESPIDER_ITEMCOUNT': 100000,
        # 'CLOSESPIDER_PAGECOUNT': 100000,
        'CLOSESPIDER_ERRORCOUNT': 100,
        'MEMUSAGE_ENABLED': True,
        # 'MEMUSAGE_LIMIT_MB': 4096,
        # 'MEMUSAGE_WARNING_MB': 1536,
        'REACTOR_THREADPOOL_MAXSIZE': 32,
        'DNS_TIMEOUT': 30,
        'DOWNLOAD_MAXSIZE': 10485760,
        'DOWNLOAD_WARNSIZE': 5242880,
        'AJAXCRAWL_ENABLED': True,
        'ROBOTSTXT_OBEY': True,
        'HTTPCACHE_ENABLED': False,
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
            'content': 'div.text-long p, div.article__body.cms-body p',
            'date': 'span.time, div.date, div.time, div.col-md-4.mb-2'
        },
        'thoidai.com.vn': {
            'title': 'h1.article-detail-title.f0, h1[class="article-detail-title f0"], h1.article-detail-title',
            'content': 'div.__MASTERCMS_CONTENT.fw.f1.mb20.clearfix p, div.article-desc',
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
            'content': '#cms-main-article > div.article__body.cms-body p, div.article__body.cms-body p, #cms-main-article > div.article__body.cms-body',
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
                
                # Define base headers
                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'vi,en-US;q=0.7,en;q=0.3',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache',
                    'Referer': 'https://www.google.com/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
                
                # Add special headers for QDND
                if 'qdnd.vn' in url:
                    logger.info(f"QDND request details:")
                    logger.info(f"Headers: {self.custom_settings['DEFAULT_REQUEST_HEADERS']}")
                    logger.info(f"Download delay: {self.custom_settings['DOWNLOAD_DELAY']}")
                    logger.info(f"Concurrent requests per domain: {self.custom_settings['CONCURRENT_REQUESTS_PER_DOMAIN']}")
                
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    errback=self.handle_error,
                    meta={
                        'download_timeout': self.request_timeout,
                        'dont_redirect': True,
                        'handle_httpstatus_list': [301, 302, 403, 404, 500, 429],
                        'dont_merge_cookies': False,
                        'download_slot': urlparse(url).hostname
                    },
                    headers=headers,
                    dont_filter=True
                )

    def handle_error(self, failure):
        url = failure.request.url
        self.failed_urls.add(url)
        
        # Add special logging for QDND
        if 'qdnd.vn' in url:
            logger.error(f"QDND error details for {url}:")
            logger.error(f"Error type: {type(failure.value).__name__}")
            logger.error(f"Error value: {str(failure.value)}")
            if hasattr(failure.value, 'response'):
                logger.error(f"Response status: {failure.value.response.status}")
                logger.error(f"Response headers: {failure.value.response.headers}")
        
        if failure.check(HttpError):
            # Xử lý HTTP error
            if failure.value.response.status in [403, 429]:
                # Rate limit, sleep longer
                sleep_time = random.uniform(10, 20)  # Tăng thời gian sleep cho QDND
                logger.warning(f"Rate limit detected for {url}, sleeping for {sleep_time} seconds")
                time.sleep(sleep_time)
        elif failure.check(TimeoutError):
            # Tăng timeout cho retry
            failure.request.meta['download_timeout'] *= 2
            logger.warning(f"Timeout for {url}, increased timeout to {failure.request.meta['download_timeout']}")

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
            
            # Handle special cases for VOV World
            if 'vovworld.vn' in hostname:
                if '/zh-CN.vov' in url or 'zh-CN' in url:
                    return 'vovworld.vn', 'zh'
                elif '/vi-VN.vov' in url or 'vi-VN' in url:
                    return 'vovworld.vn', 'vi'
                else:
                    # Default to Vietnamese for VOV World if no language specified
                    return 'vovworld.vn', 'vi'
                    
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

        # Check URL for language indicators first
        if 'vovworld.vn' in domain:
            if '/en-US.vov' in response.url:
                logger.info(f"Skipping English content for VOV World URL: {response.url}")
                return
            elif '/zh-CN.vov' in response.url:
                language_code = 'zh'
            elif '/vi-VN.vov' in response.url:
                language_code = 'vi'
            else:
                # For VOV World without language in URL, we'll check content later
                pass
        elif 'vnanet.vn' in domain:
            if '/en/' in response.url:
                logger.info(f"Skipping English content for VNA URL: {response.url}")
                return
            elif '/zh/' in response.url:
                language_code = 'zh'
            elif '/vi/' in response.url:
                language_code = 'vi'
            else:
                language_code = 'vi'  # Default to Vietnamese for VNA
            
        # Get selector key and log for debugging
        selector_key = self.get_selector_key(domain, language_code)
        logger.info(f"Processing URL: {response.url}")
        logger.info(f"Normalized domain: {domain}, Initial language code: {language_code}")
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
            logger.info(f"Detected language: {detected_lang}, Expected: {language_code} for URL: {response.url}")
            
            # Map detected language to expected format
            if detected_lang in ['zh-cn', 'zh-tw', 'zh']:
                detected_lang = 'zh'
                logger.info(f"Mapped {detected_lang} to zh for URL: {response.url}")
            elif detected_lang == 'vi':
                detected_lang = 'vi'
            else:
                logger.info(f"Skipping non-zh/vi content - Detected: {detected_lang} for URL: {response.url}")
                return
                
            # Verify language matches expected language from URL
            if language_code not in ['zh', 'vi']:
                logger.warning(f"Invalid language code from URL: {language_code} for URL: {response.url}")
                return
                
            if detected_lang != language_code:
                logger.warning(
                    f"Language mismatch for {response.url}: "
                    f"detected {detected_lang}, expected {language_code}"
                )
                # Only trust the detected language if it's zh or vi
                if detected_lang in ['zh', 'vi']:
                    language_code = detected_lang
                    logger.info(f"Updated language code to {language_code} based on detection")
                else:
                    logger.info(f"Skipping content due to language mismatch for URL: {response.url}")
                    return
            
        except LangDetectException as e:
            logger.error(f"Language detection failed for {response.url}: {str(e)}")
            # Only proceed if we have a valid language code from URL
            if language_code not in ['zh', 'vi']:
                logger.info(f"Skipping content due to invalid language code for URL: {response.url}")
                return
            logger.warning(f"Using language code from URL ({language_code}) due to detection failure")
        
        # Final validation before yielding item
        if language_code not in ['zh', 'vi']:
            logger.info(f"Skipping content with invalid language code: {language_code} for URL: {response.url}")
            return
            
        if not title or not content:
            logger.warning(f"Missing title or content for URL: {response.url}")
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
        
        # Tạo item với thông tin ngôn ngữ đầy đủ
        item = {
            'url': response.url,
            'title': title,
            'content': content,
            'date': date,
            'domain': domain,
            'language': language_code,  # This will be either 'zh' or 'vi'
            'crawled_at': datetime.now().isoformat(),
            'task_id': self.task_id,
            'meta': {
                'description': self.extract_meta_content(response, 'description'),
                'keywords': self.extract_meta_content(response, 'keywords'),
                'language': self.extract_meta_content(response, 'language'),
                'robots': self.extract_meta_content(response, 'robots'),
                'detected_language': detected_lang,  # Store detected language in meta
                'language_detection_confidence': 'high' if detected_lang == language_code else 'medium'
            }
        }
        
        logger.info(f"Crawl successful: {response.url} - Language: {language_code} - Confidence: {item['meta']['language_detection_confidence']}")
        yield item

    def closed(self, reason):
        logger.info(f"Spider finished: {reason}") 