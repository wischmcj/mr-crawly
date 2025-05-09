from __future__ import annotations

from urllib.parse import urljoin, urlparse

import redis
from bs4 import BeautifulSoup
from cache import URLCache
from config.configuration import get_logger


class Parser:
    def __init__(
        self,
        seed_url: str,
        current_url: str,
        max_pages: int = 10,
        host: str = "localhost",
        port: int = 7777,
    ):
        self.seed_url = seed_url
        self.current_url = current_url
        self.max_pages = max_pages
        self.visited_urls = set()
        self.to_visit = []
        self.logger = get_logger("crawler")
        self.host = host
        self.port = port
        self.redis_conn = redis.Redis(host=host, port=port, decode_responses=False)
        self.cache = URLCache(self.redis_conn)

    def request_page(self, url: str):
        """Get the contents of the sitemap"""
        content, req_status = self.cache.get_cached_response(url)
        if content is None or req_status != 200:
            return None
        return content

    def get_links(self, url: str) -> set[str]:
        """Extract all links from a webpage"""
        content = self.request_page(url)
        if content is None:
            self.logger.warning(f"Skipping {url} (no content cached)")
            return set()
        soup = BeautifulSoup(content, "html.parser")
        links = set()
        # Looking for <a></a> tags with an href
        # Future state: look for other linkable tags like <img> or <script>
        for anchor in soup.find_all("a", href=True):
            try:
                href = anchor["href"]
                absolute_url = urljoin(url, href)
            except Exception as e:
                self.logger.error(f"Error parsing {url}: {e}")
                return set()
            # Only include URLs from the same domain
            if urlparse(absolute_url).netloc == urlparse(url).netloc:
                links.add(absolute_url)
                links.add(url)
        return links

    def recurse_links(self, src_link: str) -> set[str]:
        """Returns the links in the page to
        Manager to be downloaded then parsed"""
        self.to_visit.append(src_link)
        links = self.get_links(src_link)
        for link in links:
            if link not in self.to_visit:
                self.to_visit.append(link)
        return links

    # Crawling Logic
    def crawl(self):
        """Main crawling method"""
        current_url = self.current_url
        self.logger.info(f"Crawling: {current_url}")
        if current_url is None:
            return None
        if not self.can_fetch(current_url):
            self.logger.info(f"Skipping {current_url} (not allowed by robots.txt)")
            return None
        self.visited_urls.add(current_url)

        new_links = self.get_links(current_url)
        return new_links


def extract_urls(args):
    """Extract URLs from a webpage"""
    seed_url, curr_url = args
    parser = Parser(seed_url, curr_url)
    new_links = parser.crawl()
    return new_links


if __name__ == "__main__":
    extract_urls("https://www.google.com")
