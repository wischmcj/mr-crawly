from __future__ import annotations

import json
from collections import defaultdict

import bs4
import redis
from bs4 import BeautifulSoup
from config.configuration import get_logger
from rq import Queue
from utils import parse_url


class SiteMapper:
    def __init__(
        self,
        seed_url: str,
        max_pages: int = 10,
        delay: float = 1.0,
        parse=True,
        queue_downloads: bool = False,
        host="localhost",
        port=7777,
    ):
        self.seed_url = seed_url
        self.max_pages = max_pages
        self.delay = delay
        self.queue_downloads = queue_downloads

        self.visited_urls: set[str] = set()
        self.seed_url = seed_url
        self.to_visit: list[str] = [seed_url]
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
        self.host = host
        self.port = port
        self.redis_conn = redis.Redis(host=self.host, port=self.port)

    def request_page(self, url: str):
        """Request the html for the given url from manager"""
        self.logger.info(f"requesting page {url}")
        page = self.redis_conn.get(url)
        self.logger.info(f"Page: {page}")
        return page

    # Link Aggregation
    def process_sitemaps(
        self, sm_soup: BeautifulSoup, sm_url: str, scheme: str, index: str = None
    ) -> set[str]:
        """Extract links from sitemap.xml if available"""

        if sm_soup.sitemapindex is not None:
            sm_locs = sm_soup.sitemapindex.find_all("loc")
            sm_urls = [sm_loc.text for sm_loc in sm_locs]
            pages = [(self.request_page(sm_url), sm_url) for sm_url in sm_urls]

            for (page_contents, status_code), sm_url in pages:
                sm_soup = BeautifulSoup(page_contents, "lxml")
                details = {}
                if sm_soup is not None:
                    details = self.process_sitemaps(sm_soup, sm_url, scheme, sm_url)
                else:
                    details["status"] = status_code
                self.sitemap_indexes[sm_url].append(details)
        else:
            details = defaultdict(list)
            details["source_url"] = sm_url
            details["index"] = index
            url = sm_soup.find("url")
            if url is not None:
                try:
                    details["loc"] = url.loc.text
                    details["priority"] = url.priority
                    details["frequency"] = url.changefreq
                    details["modified"] = url.lastmod
                    details["status"] = "200"
                except Exception as e:
                    self.logger.error(f"Error processing sitemap: {e}")
                    details["status"] = "Parsing Error"
            for key, value in details.items():
                if isinstance(value, bs4.element.Tag):
                    details[key] = value.text
            self.sitemap_details.append(dict(details))

    def get_sitemap_urls(self, url: str) -> set[str]:
        """Process a sitemap index and return all URLs found"""
        scheme, netloc, _ = parse_url(url)
        for file in [  # "sitemap-index.xml",
            "sitemap.xml"
        ]:
            sitemap_url = f"{scheme}://{netloc}/{file}"
            html = self.request_page(sitemap_url)
            soup = BeautifulSoup(html, "lxml")
            if soup is not None:
                found = file
                break
        print(f"sitemap soup: {soup}")
        if found is None:
            self.logger.warning(f"No sitemap found for {url}")
            return None
        self.process_sitemaps(soup, soup.loc.text, scheme, index="root")

        with open("google_sitemap.json", "w") as f:
            json.dump(self.sitemap_details, f, default=str, indent=4)

        return self.sitemap_details


def map_site(url: str):
    """Map a site"""
    site_mapper = SiteMapper(url)
    site_mapper.get_sitemap_urls(url)
    r = site_mapper.redis_conn
    parse_queue = Queue(connection=r, name="parse")
    _ = parse_queue.enqueue(url, args=[url])
    return site_mapper.sitemap_details
