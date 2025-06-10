import os
import sys
import time
import logging
from datetime import datetime
import psutil
from multiprocessing import Process
from typing import List
import glob
from urllib.parse import urlparse
import math

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure root logger for process management
process_handler = logging.FileHandler('logs/process.log')
process_error_handler = logging.FileHandler('logs/process_errors.log')
process_error_handler.setLevel(logging.ERROR)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Console output
        process_handler,  # Process log file
        process_error_handler  # Process error log file
    ]
)
logger = logging.getLogger(__name__)

def get_process_info(pid):
    """Get process information"""
    try:
        process = psutil.Process(pid)
        return {
            'pid': pid,
            'name': process.name(),
            'cpu_percent': process.cpu_percent(),
            'memory_percent': process.memory_percent(),
            'status': process.status()
        }
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None

def log_running_processes():
    """Log information about running processes"""
    logger.info("Current running processes:")
    for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent', 'status']):
        try:
            if 'python' in proc.info['name'].lower():
                logger.info(f"Process: {proc.info['name']} (PID: {proc.info['pid']})")
                logger.info(f"  CPU: {proc.info['cpu_percent']}%")
                logger.info(f"  Memory: {proc.info['memory_percent']:.2f}%")
                logger.info(f"  Status: {proc.info['status']}")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

def run_scrapy_spider(urls: List[str], process_id: int, collection_name: str):
    try:
        # Store original working directory to restore later
        current_dir = os.getcwd()
        logger.debug(f"Process {process_id}: Original CWD: {current_dir}")
        
        # Change to the scrapy project root directory
        os.chdir('scraper')
        logger.debug(f"Process {process_id}: Changed CWD to: {os.getcwd()}")
        logger.debug(f"Process {process_id}: sys.path before settings: {sys.path}")

        # Set the Scrapy settings module environment variable
        os.environ['SCRAPY_SETTINGS_MODULE'] = 'scraper.settings'
        logger.debug(f"Process {process_id}: SCRAPY_SETTINGS_MODULE set to: {os.environ['SCRAPY_SETTINGS_MODULE']}")

        settings = get_project_settings()
        logger.debug(f"Process {process_id}: Scrapy settings loaded. LOG_LEVEL: {settings.get('LOG_LEVEL')}")
        
        process = CrawlerProcess(settings)
        
        # Add spider to the process
        # The spider name 'news_spider' must match the name attribute in NewsSpider class
        process.crawl('news_spider', urls=urls, task_id=str(process_id), collection_name=collection_name)
        
        logger.info(f"Starting Scrapy process {process_id} with URLs: {urls}")
        process.start()  # The script will block here until all crawls are finished
        logger.info(f"Scrapy process {process_id} finished.")

    except Exception as e:
        logger.error(f"Process error for Scrapy process {process_id}: {str(e)}")
        logger.error(f"Full traceback: ", exc_info=True) # Log full traceback
    finally:
        # Change back to the original directory after the spider finishes
        os.chdir(current_dir)
        logger.debug(f"Changed back to original working directory: {os.getcwd()}")

def calculate_optimal_processes(url_count: int, total_processes: int) -> int:
    """
    Calculate optimal number of processes for a given URL count
    Args:
        url_count: Number of URLs to process
        total_processes: Total available processes (based on CPU cores)
    Returns:
        Optimal number of processes to use
    """
    if url_count == 0:
        return 0
    if url_count == 1:
        return 1
    
    # Calculate base number of processes (at least 1 URL per process)
    base_processes = min(url_count, total_processes)
    
    # If we have more URLs than processes, use all available processes
    if url_count >= total_processes:
        return total_processes
    
    # For small number of URLs, use fewer processes
    # This prevents having too many processes with very few URLs each
    return max(1, min(base_processes, math.ceil(url_count / 2)))

def distribute_urls(urls: List[str], num_processes: int) -> List[List[str]]:
    """
    Distribute URLs across processes in a round-robin fashion
    Args:
        urls: List of URLs to distribute
        num_processes: Number of processes to distribute to
    Returns:
        List of URL chunks for each process
    """
    if num_processes == 0:
        return []
    if num_processes == 1:
        return [urls]
        
    # Initialize chunks
    chunks = [[] for _ in range(num_processes)]
    
    # Distribute URLs in round-robin fashion
    for i, url in enumerate(urls):
        chunk_index = i % num_processes
        chunks[chunk_index].append(url)
    
    return chunks

def main():
    logger.info("Starting crawler system...")
    
    # Get CPU count and calculate total available processes
    cpu_count = psutil.cpu_count(logical=True)
    total_processes = min(int(cpu_count * 1.5), 8)  # Limit max processes to 8
    logger.info(f"CPU cores: {cpu_count}, Total available processes: {total_processes}")
    
    # Clean up old log files
    logger.info("Cleaning up old log files...")
    for log_file in glob.glob('logs/*.log'):
        try:
            os.remove(log_file)
            logger.info(f"Deleted old log file: {log_file}")
        except Exception as e:
            logger.warning(f"Could not delete log file {log_file}: {e}")

    os.makedirs('logs', exist_ok=True)
    logger.info("Created logs directory")
    
    list_urls_vi = [
        # "https://nhandan.vn/",
        # "https://www.qdnd.vn/",
        # "https://www.vietnamplus.vn/",
        # "https://baochinhphu.vn/",
        "https://cand.com.vn/",
        "https://vovworld.vn/vi-VN.vov",
        "https://thoidai.com.vn/",
        "https://www.sggp.org.vn/",
    ]
    list_urls_zh = [
        # "https://cn.nhandan.vn/",
        # "https://cn.qdnd.vn/",
        # "https://zh.vietnamplus.vn/",
        # "https://cn.baochinhphu.vn/",
        # "https://cn.cand.com.vn/",
        # "https://vovworld.vn/zh-CN.vov",
        # "https://shidai.thoidai.com.vn/",
        # "https://cn.sggp.org.vn/",
    ]
    
    logger.info(f"Total Vietnamese URLs: {len(list_urls_vi)}")
    logger.info(f"Total Chinese URLs: {len(list_urls_zh)}")
    
    # Calculate number of processes (one per URL)
    vi_processes = calculate_optimal_processes(len(list_urls_vi), total_processes)
    zh_processes = calculate_optimal_processes(len(list_urls_zh), total_processes)
    
    # Adjust if total processes exceed available
    total_required = vi_processes + zh_processes
    if total_required > total_processes:
        # Prioritize Chinese URLs since Vietnamese URLs are commented out
        zh_processes = total_processes
        vi_processes = 0
    
    logger.info(f"Number of processes for Vietnamese: {vi_processes}")
    logger.info(f"Number of processes for Chinese: {zh_processes}")
    
    # Distribute URLs (one per process)
    vi_chunks = distribute_urls(list_urls_vi, vi_processes)
    zh_chunks = distribute_urls(list_urls_zh, zh_processes)
    
    processes = []
    try:
        # Start Chinese crawlers first
        logger.info("Starting Chinese language crawlers...")
        for i, chunk in enumerate(zh_chunks):
            if not chunk:  # Skip empty chunks
                continue
            logger.info(f"Starting Chinese crawler {i+1}/{zh_processes} with URL: {chunk[0]}")
            p = Process(
                target=run_scrapy_spider,
                args=(chunk, i, 'auto_crawl_zh')
            )
            p.daemon = True  # Set as daemon process
            processes.append(p)
            p.start()
            time.sleep(2)  # Add delay between process starts
            log_running_processes()
        
        # Start Vietnamese crawlers
        logger.info("Starting Vietnamese language crawlers...")
        for i, chunk in enumerate(vi_chunks):
            if not chunk:  # Skip empty chunks
                continue
            logger.info(f"Starting Vietnamese crawler {i+1}/{vi_processes} with URL: {chunk[0]}")
            p = Process(
                target=run_scrapy_spider,
                args=(chunk, i + zh_processes, 'auto_crawl_vi')
            )
            p.daemon = True  # Set as daemon process
            processes.append(p)
            p.start()
            time.sleep(2)  # Add delay between process starts
            log_running_processes()
        
        # Wait for all processes with timeout
        logger.info("Waiting for all crawlers to complete...")
        for p in processes:
            try:
                p.join(timeout=14400)  # 4 hours timeout per process
                if p.is_alive():
                    logger.warning(f"Process {p.name} timed out, terminating...")
                    p.terminate()
                    p.join(timeout=5)  # Wait for termination
                    if p.is_alive():
                        p.kill()  # Force kill if still alive
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down...")
                for proc in processes:
                    if proc.is_alive():
                        proc.terminate()
                break
            except Exception as e:
                logger.error(f"Error waiting for process: {str(e)}")
                
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
    finally:
        # Cleanup any remaining processes
        for p in processes:
            if p.is_alive():
                try:
                    p.terminate()
                    p.join(timeout=5)
                    if p.is_alive():
                        p.kill()
                except:
                    pass
        
        logger.info("All crawling processes completed or terminated")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt in main, exiting...")
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
    finally:
        logger.info("Crawler system shutdown complete") 