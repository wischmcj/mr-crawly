from __future__ import annotations

from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import redis
import requests
from cache import URLCache
from config.configuration import get_logger


class SiteDownloader:
    def __init__(
        self,
        page_url: str,
        host="localhost",
        port=7777,
    ):
        self.page_url = page_url
        self.robot_parser = RobotFileParser()
        self.logger = get_logger("crawler")
        self.host = host
        self.port = port
        self.results = dict.fromkeys(
            [
                "page_url",
                "robot_parser",
            ],
            None,
        )
        self.host = host
        self.port = port
        self.redis_conn = redis.Redis(host=host, port=port, decode_responses=False)
        self.cache = URLCache(self.redis_conn)
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
            return self.robot_parser.can_fetch("*", url) or "sitemap" in url
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
        response.raise_for_status()
        return response.text, response.status_code


def download_page(seed_url: str, page_url: str):
    """Get the page from a webpage"""
    downloader = SiteDownloader(page_url=page_url)
    results = downloader.get_page_elements(page_url)
    return results
