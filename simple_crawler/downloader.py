from __future__ import annotations

from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from config.configuration import get_logger
from manager import Manager

logger = get_logger("downloader")


class SiteDownloader:
    def __init__(self, manager: Manager):
        self.robot_parser = RobotFileParser()
        self.manager = manager
        self.cache = manager.cache
        self.visit_tracker = manager.visit_tracker
        self.db_manager = manager.db_manager

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

    def on_success(self, url: str, content: str, status_code: int):
        self.db_manager.update_content(url, content, status_code)
        self.cache.update_status(url, "downloaded")

    def on_failure(self, url: str, crawl_status: str, content: str, status_code: int):
        self.db_manager.update_content(url, content, status_code)
        data = self.cache.update_status(url, crawl_status)
        self.db_manager.store_url(data, self.manager.run_id, self.manager.seed_url)

    def get_page_elements(self, url: str) -> set[str]:
        """Get the page elements from a webpage"""
        # Add page to visited tracker
        self.visit_tracker.add_page_visited(url)

        # Check for an already cached response from previous runs
        content, status = self.cache.get_cached_response(url)
        if content is not None:
            return content, status

        # Check if we're allowed to crawl the page
        if not self.can_fetch(url):
            msg = f"Skipping {url} (not allowed by robots.txt)"
            logger.info(msg)
            self.on_failure(url, "disallowed", "", 403)
            return "", 403

        # Get the page elements
        response = requests.get(url, timeout=10)
        logger.debug(f"Getting elements for: {url}")
        try:
            response.raise_for_status()
            self.on_success(url, response.text, response.status_code)
        except Exception as e:
            # If we can't get the page, we'll return the error
            # and closed the url out, not passing it to the parser
            logger.error(f"Error getting {url}: {e}")
            self.on_failure(url, "error", response.text, response.status_code)
            return None, response.status_code
        return response.text, response.status_code
