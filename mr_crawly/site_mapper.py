from __future__ import annotations

import json
from collections import defaultdict
from urllib.robotparser import RobotFileParser

import bs4
from bs4 import BeautifulSoup
from config.configuration import get_logger
from manager import Manager
from utils import parse_url


class SiteMapParser:
    def __init__(
        self,
        manager: Manager,
        seed_url: str,
        max_pages: int = 10,
        delay: float = 1.0,
        parse=True,
    ):
        self.manager = manager

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

    def request_page(self, url: str):
        """Request the html for the given url from manager"""
        return self.manager.get_page(url)

    # Link Aggregation
    def process_sitemaps(
        self, sm_soup: BeautifulSoup, sm_url: str, scheme: str, index: str = None
    ) -> set[str]:
        """Extract links from sitemap.xml if available"""

        if sm_soup.sitemapindex is not None:
            sm_locs = sm_soup.sitemapindex.find_all("loc")
            sm_urls = [sm_loc.text for sm_loc in sm_locs]
            pages = [
                (self.request_page(sm_url, "lxml", scheme), sm_url)
                for sm_url in sm_urls
            ]

            for (sm_soup, _, status_code), sm_url in pages:
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

    def process_sitemap_index(self, url: str) -> set[str]:
        """Process a sitemap index and return all URLs found"""
        scheme, netloc, _ = parse_url(url)
        for file in ["sitemap-index.xml", "sitemap.xml"]:
            sitemap_url = f"{scheme}://{netloc}/{file}"
            html, _ = self.request_page(sitemap_url, "lxml")
            soup = BeautifulSoup(html, "lxml")
            if soup is not None:
                found = file
                break

        if found is None:
            self.logger.warning(f"No sitemap found for {url}")
            return None
        self.process_sitemaps(soup, soup.loc.text, scheme, index="root")

        with open("google_sitemap.json", "w") as f:
            json.dump(self.sitemap_details, f, default=str, indent=4)
