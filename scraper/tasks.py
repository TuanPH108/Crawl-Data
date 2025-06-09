from celery import shared_task
import logging

logger = logging.getLogger(__name__)

@shared_task(name='scraper.tasks.crawl_news')
def crawl_news(url):
    """
    Task to crawl news from a given URL
    """
    try:
        logger.info(f"Starting to crawl news from: {url}")
        # TODO: Implement actual crawling logic here
        return {"status": "success", "url": url}
    except Exception as e:
        logger.error(f"Error crawling {url}: {str(e)}")
        return {"status": "error", "url": url, "error": str(e)} 