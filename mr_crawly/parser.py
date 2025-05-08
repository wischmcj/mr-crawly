from __future__ import annotations

import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from config.configuration import get_logger


class Parser:
    def __init__(self, manager, seed_url: str, max_pages: int = 10, delay: float = 1.0):
        self.manager = manager
        self.seed_url = seed_url
        self.max_pages = max_pages
        self.delay = delay
        self.visited_urls = set()
        self.to_visit = [seed_url]
        self.logger = get_logger("crawler")

    def request_page(self, url: str) -> tuple[str, int]:
        """Request page HTML from manager"""
        return self.manager.get_page(url)

    def send_links(self, links: set[str]) -> None:
        """Send aggregated links to manager for processing"""
        self.manager.enqueue_link_aggregation(links)

    def get_links(self, url: str) -> set[str]:
        """Extract all links from a webpage"""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            links = set()

            # Looking for <a></a> tags with an href
            # Future state: look for other linkable tags like <img> or <script>
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"]
                absolute_url = urljoin(url, href)

                # Only include URLs from the same domain
                if urlparse(absolute_url).netloc == urlparse(url).netloc:
                    links.add(absolute_url)

            return links
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return set()

    def recurse_links(self, src_link: str) -> set[str]:
        """Recursively get links from a webpage"""
        self.to_visit.append(src_link)
        links = self.get_links(src_link)
        for link in links:
            if link not in self.to_visit:
                self.to_visit.append(link)
        return links

    def aggregate_links(self, url: str) -> set[str]:
        """Aggregate links from sitemap and webpage"""
        # Get sitemap/hompage links
        webpage_links = self.get_links(url)
        distinct_links = set(webpage_links)
        # recursively get links
        for link in distinct_links:
            _ = self.get_links(link)

    # Crawling Logic
    def crawl(self):
        """Main crawling method"""
        while self.to_visit and len(self.visited_urls) < self.max_pages:
            current_url = self.to_visit.pop(0)

            if current_url in self.visited_urls:
                continue

            if not self.can_fetch(current_url):
                self.logger.info(f"Skipping {current_url} (not allowed by robots.txt)")
                continue

            self.logger.info(f"Crawling: {current_url}")
            self.visited_urls.add(current_url)

            if self.parse:
                # Get new links and add them to the queue
                new_links = self.get_links(current_url)
                self.to_visit.extend(
                    link for link in new_links if link not in self.visited_urls
                )

            # Respect rate limiting
            time.sleep(self.delay)
