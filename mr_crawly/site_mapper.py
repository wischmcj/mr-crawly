from __future__ import annotations

import json
import os
import sys
from collections import defaultdict

from mr_crawly.data import SitemapTable

cwd = os.getcwd()
loc = os.path.dirname(os.path.dirname(__file__))
print(loc)
sys.path.append(loc)

import bs4
from bs4 import BeautifulSoup
from config.configuration import get_logger

from mr_crawly.cache import CrawlStatus, URLCache
from mr_crawly.site_downloader import SiteDownloader
from mr_crawly.utils import parse_url


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
        self.cache = URLCache(host=host, port=port, decode_responses=False)
        self.sitemap_table = SitemapTable(
            db_path=os.path.join(loc, "data", "sqlite.db")
        )
        self.downloader = SiteDownloader(seed_url=seed_url, host=host, port=port)

    def request_page(self, url: str):
        """Get the contents of the sitemap"""
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
                return None

        if contents is None:
            self.logger.warning(f"No sitemap found for {url}")
            return None

        self.process_sitemaps(sitemap_url, scheme, index="root")
        self.cache.update_url(sitemap_url, "status", CrawlStatus.SITE_MAP.value)

        self.logger.info(
            "Writing sitemap data to sqlite and the top level urls to rdb cache"
        )
        for detail in self.sitemap_details:
            self.sitemap_table.store_sitemap_data(detail)
            try:
                fseed = detail.get("loc")
                ## Save these as frontier seeds
                self.cache.add_frontier_seed(url, fseed)
            except:
                pass

        with open("data/" + url + "_sitemap_indexes.json", "w") as f:
            json.dump(self.sitemap_indexes, f, default=str, indent=4)

        return self.sitemap_indexes


def map_site(url: str):
    """Map a site"""
    site_mapper = SiteMapper(url)
    indexes = site_mapper.get_sitemap_urls(url)

    # parse_queue = Queue(connection=r, name="parse")
    # _ = parse_queue.enqueue(url, args=[url])
    # return site_mapper.sitemap_details


if __name__ == "__main__":
    map_site("https://www.google.com")
