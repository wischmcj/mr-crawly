from __future__ import annotations

from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from config.configuration import get_logger

from simple_crawler.manager import Manager

# from utils import BaseWorkClass


logger = get_logger("parser")


class Parser:
    def __init__(self, manager: Manager, url: str = None):
        self.url = url
        self.manager = manager
        self.cache = manager.cache
        self.visit_tracker = manager.visit_tracker
        self.db_manager = manager.db_manager

    def get_links_from_content(self, url: str, content: str) -> set[str]:
        """Extract all links from a webpage"""
        soup = BeautifulSoup(content, "html.parser")
        links = set()
        # Looking for <a></a> tags with an href
        # Future state: look for other linkable tags like <img> or <script>
        tag_instances = soup.find_all("a", href=True)
        logger.debug(f"Found {len(tag_instances)} anchor tags")
        for tag in soup.find_all("a", href=True):
            try:
                href = tag["href"]
                absolute_url = urljoin(url, href)
            except Exception as e:
                logger.error(f"Error parsing {url}: {e}")
                return set()
            # Only include URLs from the same domain
            if urlparse(absolute_url).netloc == urlparse(url).netloc:
                links.add(absolute_url)
                self.visit_tracker.add_to_visit(absolute_url)
        return links

    def on_success(self, url):
        """Callback for when a job succeeds"""
        self.cache.update_status(url, "parsed")

    def on_failure(self, url):
        """Callback for when a job fails"""
        data = self.cache.update_status(url, "error")
        self.db_manager.store_url(data, self.manager.run_id, self.manager.seed_url)

    # Crawling Logic
    def parse(self, url, content):
        """Main crawling method"""
        logger.info(f"Crawling: {url}")
        try:
            self.get_links_from_content(url, content)
            self.on_success(url)
        except Exception as e:
            logger.error(f"Error parsing {url}: {e}")
            self.on_failure(url)


if __name__ == "__main__":
    manager = Manager(
        host="localhost", port=7777, db_file="test.db", rdb_file="test.rdb"
    )
    parser = Parser(manager)
    parser.parse("https://www.google.com", "https://www.overstory.com")
