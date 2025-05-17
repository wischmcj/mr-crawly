from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum

import redis
from config.configuration import get_logger  # noqa

logger = get_logger("data")


class CrawlStatus(Enum):
    """Enum for tracking URL crawl status"""

    SITE_MAP = "map_site"
    FRONTIER = "frontier"
    PARSE = "parse"
    DB = "db"
    ERROR = "error"
    CLOSED = "closed"
    DISALLOWED = "disallowed"


class UrlAttributes(Enum):
    """Enum for tracking URL attributes"""

    HTML = "html"
    SITEMAP_FREQUENCY = "sitemap_frequency"
    SITEMAP_PRIORITY = "sitemap_priority"
    LAST_MODIFIED = "last_modified"
    STATUS = "status"


@dataclass
class URLData:
    """`Data` structure for URL metadata"""

    # URL first enters the cache when it is 'visited'
    # e.g. a download attempt is made
    url: str
    content: str = ""
    req_status: CrawlStatus = CrawlStatus.FRONTIER
    crawl_status: CrawlStatus = CrawlStatus.FRONTIER
    run_id: str = ""


class CrawlTracker:
    """Track the status of a URL"""

    def __init__(self, manager, url_pubsub):
        self.manager = manager
        self.rdb = manager.rdb
        self.seed_url = manager.seed_url
        self.run_id = manager.run_id
        self.urls = defaultdict(dict)
        self.completed_pages = 0
        self.max_pages = manager.max_pages
        self.visited_urls = set()

    def store_linked_urls(self, parent_url: str, links: list[str]) -> None:
        """Update the parent URL for a URL"""
        self.urls[parent_url]["linked_urls"] = links

    def close_url(self, url_data: str) -> None:
        """Close a URL"""
        self.completed_pages += 1
        if self.completed_pages >= self.max_pages:
            self.rdb.publish("db", "exit")
        else:
            self.rdb.publish("db", json.dumps(url_data))
        return url_data

    def update_status(self, url: str, status: str, status_code: int = None) -> None:
        """
        Progresses the status of the URL through the crawl pipeline.
        If and error state is passed, the url is closed and removed from the cache.
        """
        if status in ("error", "disallowed", "parse"):
            url_data = self.urls.pop(url, {})
        else:
            url_data = self.urls.get(url, {})

        if url_data.get("seed_url") != self.seed_url:
            url_data["seed_url"] = self.seed_url
        if url_data.get("run_id") != self.run_id:
            url_data["run_id"] = self.run_id
        if url_data.get("crawl_status") != "started":
            url_data["crawl_status"] = "started"
        if url_data.get("url") != url:
            url_data["url"] = url

        if status_code:
            url_data["req_status"] = status_code

        if status == "downloaded":
            url_data["crawl_status"] = "parse"
        elif status == "parsed":
            url_data["crawl_status"] = "Finished"
            self.close_url(url_data)
        elif status == "error" or status == "disallowed":
            url_data["crawl_status"] = status
            self.close_url(url_data)
        url_data["crawl_status"] = status
        self.urls[url] = url_data
        return url_data

    def close_queue(self) -> None:
        """Close the queue"""
        self.sent_exit = True

    def get_page_to_visit(self) -> list[str]:
        """Get all frontier seeds for a URL"""
        if self.completed_pages >= self.max_pages:
            logger.warning("Max pages reached, closing queue")
            return None
        url = self.rdb.lpop("to_visit")
        if url is not None:
            url = url.decode("utf-8")
        return url or ""

    def add_page_to_visit(self, url: str) -> None:
        """Add a frontier URL to the visit queue"""
        if url not in self.visited_urls:
            self.rdb.lpush("to_visit", url)

    def add_page_visited(self, url: str) -> None:
        """Add a visited seed for a URL"""
        self.visited_urls.add(url)


class URLCache:
    """
    Cache for URL content and request_status
    """

    def __init__(self, redis_conn: redis.Redis):
        self.rdb = redis_conn
        self.queues = []
        self.visited_urls = set()
        self.to_visit = set()

    def update_content(self, url: str, content, status) -> None:
        """
        Store URL data in cache
        Note that the url may or may not be in the cache
        """
        if not isinstance(content, str):
            raise ValueError(f"Content must be a string, got {type(content)}")
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
