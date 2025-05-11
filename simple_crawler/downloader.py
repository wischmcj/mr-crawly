from __future__ import annotations

import logging
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import redis
import requests
from cache import URLCache, VisitTracker
from config.configuration import get_logger

logger = get_logger(__name__)


class SiteDownloader:
    def __init__(
        self,
        page_url: str,
        host="localhost",
        port=7777,
        logger=None,
    ):
        self.page_url = page_url
        self.robot_parser = RobotFileParser()
        logger = logger
        if logger is None:
            logger = logging.getLogger(__name__)
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
        self.tracker = VisitTracker(self.redis_conn)
        self.cache = URLCache(self.redis_conn)
        self.work_class = "download"

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
            logger.warning(f"Error checking robots.txt for {url}: {e}")
            return True  # If we can't check robots.txt, we probably want to set a reasonable default

    def read_politeness_info(self, url: str):
        parsed_url = urlparse(url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        self.robot_parser.set_url(robots_url)
        self.robot_parser.read()
        self.rrate = self.robot_parser.request_rate("*")
        self.crawl_delay = self.robot_parser.crawl_delay("*")

    def get_page_elements(self, url: str) -> set[str]:
        """Get the page elements from a webpage"""
        self.tracker.add_page_visited(url)
        if not self.can_fetch(url):
            msg = f"Skipping {url} (not allowed by robots.txt)"
            logger.info(msg)
            raise PermissionError(msg)
        response = requests.get(url, timeout=10)
        logger.debug(f"Getting elements for: {url}")
        response.raise_for_status()
        self.cache.update_content(url, response.text, response.status_code)
        return response.text, response.status_code
