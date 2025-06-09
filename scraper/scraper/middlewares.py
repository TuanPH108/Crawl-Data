from fake_useragent import UserAgent
import random
import time

class RandomUserAgentMiddleware:
    def __init__(self):
        self.ua = UserAgent()

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_request(self, request, spider):
        request.headers.setdefault('User-Agent', self.ua.random)

class RandomDelayMiddleware:
    def __init__(self, min_delay=1, max_delay=3):
        self.min_delay = min_delay
        self.max_delay = max_delay

    @classmethod
    def from_crawler(cls, crawler):
        min_delay = crawler.settings.getint('RANDOM_DELAY_MIN', 1)
        max_delay = crawler.settings.getint('RANDOM_DELAY_MAX', 3)
        return cls(min_delay, max_delay)

    def process_request(self, request, spider):
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay) 