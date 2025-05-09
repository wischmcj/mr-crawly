from __future__ import annotations

import os
import sys
from collections import defaultdict

import redis

cwd = os.getcwd()
loc = os.path.dirname(os.path.dirname(__file__))
print(loc)
sys.path.append(loc)

import bs4  # noqa
from bs4 import BeautifulSoup  # noqa
from cache import URLCache  # noqa
from config.configuration import get_logger  # noqa
from site_downloader import SiteDownloader  # noqa
from utils import parse_url  # noqa


class SiteMapper:
    def __init__(
        self,
        seed_url: str,
        parse=True,
        host="localhost",
        port=7777,
    ):
        self.seed_url = seed_url
        self.seed_url = seed_url
        self.to_visit: list[str] = [seed_url]
        self.logger = get_logger("crawler")
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
        self.frontier = []
        self.host = host
        self.port = port
        self.redis_conn = redis.Redis(host=host, port=port, decode_responses=False)
        self.cache = URLCache(self.redis_conn)
        self.downloader = SiteDownloader(page_url=seed_url, host=host, port=port)

    def request_page(self, url: str):
        """We allow a direct connection here given the limited
        number of pages we are requesting as part of this process
        """
        content, status = self.cache.get_cached_response(url)
        if content is None:
            content, req_status = self.downloader.get_page_elements(url)
            if req_status != 200:
                return None
        return content

    # Link Aggregation
    def process_sitemaps(
        self, cur_url: str, scheme: str, index: str = None
    ) -> set[str]:
        """Extract links from sitemap.xml if available"""
        contents = self.request_page(cur_url)
        if contents is None:
            return None
        sm_soup = BeautifulSoup(contents, "lxml")
        if sm_soup.find("sitemapindex") is not None:
            # Page is a sitemapindex and locs represent more
            # sitemap urls that need to be passed to the downloader
            sm_urls = [loc.text for loc in sm_soup.find_all("loc")]
            self.sitemap_indexes[cur_url].extend(sm_urls)
            self.logger.info(f"New Sitemap URLs: {sm_urls}")
            for sm_url in sm_urls:
                details = self.process_sitemaps(sm_url, scheme, index=cur_url)
        else:
            # Page is a sitemap and sites represent
            # seed urls for the crawler
            details = defaultdict(list)
            details["source_url"] = cur_url
            details["index"] = index
            url = sm_soup.find("url")
            if url is not None:
                try:
                    details["loc"] = url.loc
                    details["priority"] = url.priority
                    details["frequency"] = url.changefreq
                    details["modified"] = url.lastmod
                    details["status"] = "Success"
                except Exception as e:
                    self.logger.error(f"Error processing sitemap: {e}")
                    details["status"] = "Parsing Error"
            for key, value in details.items():
                if isinstance(value, bs4.element.Tag):
                    details[key] = value.text

            # Save the sitemap details for the db
            self.sitemap_indexes[cur_url].append(cur_url)
            self.sitemap_details.append(dict(details))

            # Push url to the queue to be downloaded
            self.cache.request_download(details.get("loc"))

    def get_sitemap_urls(self, url: str) -> str:
        """Process a sitemap index and return all URLs found"""
        scheme, netloc, _ = parse_url(url)
        sitemap_url = f"{scheme}://{netloc}/sitemap-index.xml"
        contents = self.request_page(sitemap_url)
        if contents is None:
            sitemap_url = f"{scheme}://{netloc}/sitemap.xml"
            contents = self.request_page(sitemap_url)
            if contents is None:
                self.logger.warning(f"No sitemap found for {url}")
                self.sitemap_indexes = None
                raise Exception(f"No sitemap found for {url}")

        self.process_sitemaps(sitemap_url, scheme, index="root")

        return sitemap_url, self.sitemap_indexes, self.sitemap_details


def map_site(url: str):
    """Map a site"""
    site_mapper = SiteMapper(url)
    result = site_mapper.get_sitemap_urls(url)
    return result


if __name__ == "__main__":
    map_site("https://www.google.com")
