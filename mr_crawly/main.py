from __future__ import annotations

import argparse
import time
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup


class WebCrawler:
    def __init__(self, start_url: str, max_pages: int = 10, delay: float = 1.0):
        self.start_url = start_url
        self.max_pages = max_pages
        self.delay = delay
        self.visited_urls: set[str] = set()
        self.to_visit: list[str] = [start_url]
        self.robot_parser = RobotFileParser()

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
            return True  # If we can't check robots.txt, proceed with caution

    def get_links(self, url: str) -> set[str]:
        """Extract all links from a webpage"""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            links = set()

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

            # Get new links and add them to the queue
            new_links = self.get_links(current_url)
            self.to_visit.extend(
                link for link in new_links if link not in self.visited_urls
            )

            # Respect rate limiting
            time.sleep(self.delay)


def main():
    parser = argparse.ArgumentParser(description="Basic Web Crawler")
    parser.add_argument("url", help="Starting URL to crawl")
    parser.add_argument(
        "--max-pages", type=int, default=10, help="Maximum number of pages to crawl"
    )
    parser.add_argument(
        "--delay", type=float, default=1.0, help="Delay between requests in seconds"
    )

    args = parser.parse_args()

    crawler = WebCrawler(start_url=args.url, max_pages=args.max_pages, delay=args.delay)

    crawler.crawl()


if __name__ == "__main__":
    main()
