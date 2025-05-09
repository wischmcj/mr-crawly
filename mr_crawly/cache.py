from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import redis
import rq


class CrawlStatus(Enum):
    """Enum for tracking URL crawl status"""

    FRONTIER = "frontier"
    SITE_MAP = "site_map"
    PARSE = "parse"
    DB = "db"
    ERROR = "error"


class UrlAttributes(Enum):
    """Enum for tracking URL attributes"""

    HTML = "html"
    SITEMAP_FREQUENCY = "sitemap_frequency"
    SITEMAP_PRIORITY = "sitemap_priority"
    LAST_MODIFIED = "last_modified"
    STATUS = "status"


class SitemapDetails(Enum):
    """Enum for tracking sitemap details"""

    SOURCE_URL = "source_url"
    INDEX = "index"
    LOC = "loc"
    PRIORITY = "priority"
    FREQUENCY = "frequency"
    MODIFIED = "modified"
    STATUS = "status"


@dataclass
class URLData:
    """Data structure for URL metadata"""

    html: str | None = None
    download_status: dict | None = None
    source_url: str | None = None
    sitemap_frequency: str | None = None
    sitemap_priority: float | None = None
    last_modified: str | None = None
    status: CrawlStatus = CrawlStatus.FRONTIER
    is_sitemap: bool | None = None
    is_sitemap_index: bool | None = None


class URLCache:
    """Redis-based cache for URL data"""

    def __init__(
        self, host: str = "localhost", port: int = 7777, decode_responses: bool = False
    ):
        self.rdb = redis.Redis(host=host, port=port, decode_responses=decode_responses)
        self.host = host
        self.port = port
        self.queues = []
        # self.rdb.flushdb()

    def add_queue(self, queue: rq.Queue) -> None:
        """Add a queue to the cache"""
        self.queues.append(queue)

    def get_queue(self, name: str) -> rq.Queue:
        """Get a queue from the cache"""
        for queue in self.queues:
            if queue.name == name:
                return queue
        return None

    def decode_data(self, data: dict) -> URLData:
        """Decode data from cache"""
        for key, value in data.items():
            new_val = value
            new_key = key
            if isinstance(value, bytes):
                new_val = value.decode("utf-8")
            if isinstance(key, bytes):
                new_key = key.decode("utf-8")
            data[new_key] = new_val
        return data

    def update_url(self, url: str, attr: URLData, value) -> None:
        """Store URL data in cache"""
        self.rdb.hset(url, attr, value)

    def update_content(self, url: str, content, status) -> None:
        """Store URL data in cache"""
        self.rdb.hset(url, "content", content)
        self.rdb.hset(url, "req_status", status)

    def get_cached_response(self, url: str) -> URLData | None:
        """Retrieve URL data from cache"""
        content, status = None, None
        bcontent = self.rdb.hget(url, "content")
        bstatus = self.rdb.hget(url, "req_status")
        if bcontent:
            content = bcontent.decode("utf-8")
        if bstatus:
            status = bstatus.decode("utf-8")
        return content, status
        # Deserialize and convert back to URLData
        # data_dict = json.loads(data)
        # data_dict["status"] = CrawlStatus(data_dict["status"])
        # return URLData(**data_dict)

    def get_all_urls(self) -> dict[str, URLData]:
        """Get all cached URL data"""
        all_data = self.rdb.hgetall("urls")
        results = {}

        for url, data in all_data.items():
            data_dict = json.loads(data)
            data_dict["status"] = CrawlStatus(data_dict["status"])
            results[url] = URLData(**data_dict)

        return results

    def update_status(self, url: str, status: CrawlStatus) -> None:
        """Update just the crawl status for a URL"""
        data = self.get_url_data(url)
        if data:
            data.status = status
            self.set_url_data(url, data)

    def request_download(self, seed_url: str, url: str) -> list[str]:
        """Request a download for a URL"""
        self.rdb.publish(f"{seed_url}:download_needed", url)

    def add_frontier_seed(self, url: str, seed: str) -> None:
        """Add a frontier seed for a URL"""
        self.rdb.sadd(f"{url}:fseeds", seed)

    def get_pages_to_parse(self, url: str) -> list[str]:
        """Get all frontier seeds for a URL"""
        return self.rdb.smembers(f"{url}:to_parse")

    def add_page_to_parse(self, url: str, seed: str) -> None:
        """Add a frontier seed for a URL"""
        self.rdb.sadd(f"{url}:to_parse", seed)


def transfer_to_db(url: str, data: URLData, run_id: int) -> None:
    """Transfer URL data to database"""
    db = UrlTable()
    db.store_url_content(url, data.html, data.run_id)
    db.insert_url(url, data)
