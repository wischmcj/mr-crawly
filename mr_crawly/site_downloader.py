from __future__ import annotations

from collections import defaultdict
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import redis
import requests
from config.configuration import get_logger
from rq import Queue

from mr_crawly.cache import CrawlStatus, URLCache


class SiteDownloader:
    def __init__(
        self,
        seed_url: str,
        host="localhost",
        port=7777,
    ):
        self.seed_url = seed_url
        self.robot_parser = RobotFileParser()
        self.logger = get_logger("crawler")
        self.host = host
        self.port = port
        self.results = dict.fromkeys(
            [
                "seed_url",
                "sitemap_index",
                "sitemap",
                "parse",
                "visited_urls",
                "to_visit",
                "robot_parser",
            ],
            None,
        )
        self.host = host
        self.port = port
        self.cache = URLCache(host=host, port=port, decode_responses=False)
        # self.frontier_urls = self.cache.get_frontier_seeds(self.seed_url)

    def save_html(self, html: str, filename: str):
        with open(filename, "w", encoding="UTF-8") as f:
            f.write(html)

    # Politeness
    def can_fetch(self, url: str) -> bool:
        """Check if we're allowed to crawl this URL according to robots.txt"""
        parsed_url = urlparse(url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        try:
            self.robot_parser.set_url(robots_url)
            self.robot_parser.read()
            return (
                self.robot_parser.can_fetch("*", url)
                or 'sitemap' in url
            )
        except Exception as e:
            self.logger.warning(f"Error checking robots.txt for {url}: {e}")
            return True  # If we can't check robots.txt, we probably want to set a reasonable default

    def get_page_elements(self, url: str) -> set[str]:
        """Get the page elements from a webpage"""
        if not self.can_fetch(url):
            self.logger.info(f"Skipping {url} (not allowed by robots.txt)")
            return None, "403"

        response = requests.get(url, timeout=10)
        self.logger.debug(f"Getting elements for: {url}")
        try:
            response.raise_for_status()
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None, response.status_code
        return response.text, response.status_code


def download_page(page_url: str):
    """Get the page from a webpage"""
    downloader = SiteDownloader(seed_url=seed_url)
    cache = SiteDownloader.cache
    content, req_status = downloader.get_page_elements(url)
    r = downloader.redis_conn
    if req_status == 200:
        # Store the content and update the status
        cache.update_content(url, content, req_status)
        cache.update_url(url,'status', CrawlStatus.SITE_MAP.value)
        queue = Queue(connection=r, name='site_map')
        # Add the successful url to the parse_queue
        cache.add_page_to_parse(seed_url, url)
    else:
        cache.update_url(url,'status', CrawlStatus.ERROR.value)
        queue = Queue(connection=r, name='db')
        _ = queue.enqueue(url, args=[url])
        # The below prevents the job from proceeding further in the pipeline
        raise Exception(f"Error downloading {url}")

    cache.queues = []
    for url in downloader.frontier_urls:
