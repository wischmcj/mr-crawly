from __future__ import annotations

from collections import defaultdict
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from config.configuration import get_logger


class SiteDownloader:
    def __init__(
        self, seed_url: str, max_pages: int = 10, delay: float = 1.0, parse=True
    ):
        self.seed_url = seed_url
        self.max_pages = max_pages
        self.delay = delay
        self.visited_urls: set[str] = set()
        self.seed_url = seed_url
        self.to_visit: list[str] = [seed_url]
        self.robot_parser = RobotFileParser()
        self.logger = get_logger("crawler")
        self.parse = parse
        self.results = dict.fromkeys(
            [
                "seed_url",
                "sitemap_index",
                "sitemap",
                "max_pages",
                "delay",
                "parse",
                "visited_urls",
                "to_visit",
                "robot_parser",
            ],
            None,
        )
        self.sitemap_details = []
        self.sitemap_indexes = defaultdict(list)

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
            return self.robot_parser.can_fetch("*", url)
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

        return response, response.status_code
