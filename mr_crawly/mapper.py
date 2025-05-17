from __future__ import annotations

import json
from collections import defaultdict

import bs4  # noqa
from bs4 import BeautifulSoup  # noqa
from config.configuration import get_logger
from downloader import SiteDownloader
from manager import Manager  # noqa
from utils import parse_url  # noqa

logger = get_logger("mapper")

SITEMAP_FEILDS = ["loc", "priority", "changefreq", "modified"]


class SiteMapper:
    def __init__(self, manager: Manager, seed_url: str, write_to_db: bool = True):
        self.manager = manager
        self.seed_url = seed_url
        self.cache = manager.cache
        self.db_manager = manager.db_manager
        self.crawl_tracker = manager.crawl_tracker
        self.downloader = SiteDownloader(manager, write_to_db)
        self.write_to_db = write_to_db

        self.sitemap_indexes = defaultdict(list)
        self.sitemap_details = []
        self.sitemap_feilds = SITEMAP_FEILDS

    def save_html(self, html: str, filename: str):
        with open(filename, "w", encoding="UTF-8") as f:
            f.write(html)

    def request_page(self, url: str):
        """We allow a direct connection here given the limited
        number of pages we are requesting as part of this process
        """
        content, status = self.cache.get_cached_response(url)
        if content is None:
            logger.debug(f"No content cached for {url}")
            try:
                content, req_status = self.downloader.get_page_elements(
                    url, cache_results=False
                )
                logger.debug(f"Content received from downloader for {url}")
            except Exception as e:
                logger.debug(f"Error getting page for {url}: {e}")
                return None
            if req_status != 200:
                return None
        return content

    def parse_sitemap_index(self, url: str, soup: BeautifulSoup):
        """Parse the sitemap"""
        sm_urls = [loc.text for loc in soup.find_all("loc")]
        self.sitemap_indexes[url].extend(sm_urls)
        logger.info(f"New Sitemap URLs: {sm_urls}")
        return sm_urls

    # Link Aggregation
    def process_sitemap(
        self, cur_url: str, soup: BeautifulSoup, index: str = None
    ) -> set[str]:
        """Extract links from sitemap.xml if available"""
        details = defaultdict(list)
        details["source_url"] = cur_url
        details["index"] = index
        url = soup.find("url")
        if url is not None:
            details["status"] = "Success"
            for field in self.sitemap_feilds:
                details[field] = url.find(field)

        for key, value in details.items():
            if isinstance(value, bs4.element.Tag):
                details[key] = value.text
        return details

    def recurse_sitemap(self, url: str, contents: str, index: str = None):
        """Recurse through the sitemap"""
        sm_soup = BeautifulSoup(contents, features="lxml")
        try:
            if sm_soup.find("sitemapindex") is not None:
                links = self.parse_sitemap_index(url, sm_soup)
                index = url
                for link in links:
                    content = self.request_page(link)
                    if content is None:
                        logger.debug(f"No content available for {link}")
                        continue
                    self.recurse_sitemap(link, content, index)
            else:
                details = self.process_sitemap(url, sm_soup)
                self.sitemap_indexes[index].append(url)
                self.sitemap_details.append(dict(details))
                if details.get("status") == "Success":
                    self.manager.crawl_tracker.add_page_to_visit(details.get("loc"))
                links = [details.get("loc")]

        except Exception as e:
            logger.error(f"Error parsing {url}: {e}")
            return

    def get_sitemap_urls(self, sitemap_url: str) -> str:
        """Process a sitemap index and return all URLs found"""
        logger.info(f"Getting sitemap urls for {sitemap_url}")
        contents = self.request_page(sitemap_url)
        if contents is None:
            logger.warning(f"No sitemap found for {sitemap_url}")
            raise Exception(f"No sitemap found for {sitemap_url}")
        self.recurse_sitemap(sitemap_url, contents, index="root")
        return sitemap_url, self.sitemap_indexes, self.sitemap_details

    # Map site specific on end functions
    def on_map_success(self, result):
        """Callback for when a site mapping job succeeds"""
        _, sitemap_indicies, sitemap_details = result
        logger.info("Writing sitemap data to sqlite, index data to file")

        with open(f"{self.manager.data_dir}/sitemap_indexes.json", "w") as f:
            json.dump(sitemap_indicies, f, default=str, indent=4)

        for detail in sitemap_details:
            self.db_manager.sitemap_table.store_sitemap_data(
                detail, self.manager.run_id, self.manager.seed_url
            )

    def get_sitemap(self):
        """
        Queues the download, map_site, and parse_page jobs for the given url.
        Returns the download, map_site, and parse_page jobs.
        """
        seed_url = self.manager.seed_url
        scheme, netloc, _ = parse_url(seed_url)
        sitemap_source_url = f"{scheme}://{netloc}/sitemap-index.xml"
        sitemap_urls, _, _ = self.downloader.read_politeness_info(sitemap_source_url)
        sitemaps = [x for x in sitemap_urls]

        if len(sitemaps) == 0:
            sitemap_source_url = f"{scheme}://{netloc}/sitemap-index.xml"
        else:
            sitemap_source_url = sitemaps[0]
        try:
            result = self.get_sitemap_urls(sitemap_source_url)
        except Exception as e:
            logger.error(
                f"Failed to get sitemap index for {seed_url}: {e}. Trying sitemap.xml"
            )
            sitemap_source_url = f"{scheme}://{netloc}/sitemap.xml"
            try:
                result = self.get_sitemap_urls(sitemap_source_url)
            except Exception as e:
                logger.error(f"Sitemap at {sitemap_source_url} not found: {e}")
                result = (None, [seed_url], {})
                raise e
        self.on_map_success(result)
        return result
