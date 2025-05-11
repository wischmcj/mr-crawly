from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import redis
from config.configuration import get_logger  # noqa

logger = get_logger(__name__)


class CrawlStatus(Enum):
    """Enum for tracking URL crawl status"""

    SITE_MAP = "map_site"
    FRONTIER = "frontier"
    PARSE = "parse"
    DB = "db"
    ERROR = "error"
    CLOSED = "closed"


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

    def __init__(self, redis_conn: redis.Redis):
        self.rdb = redis_conn

    def set_url_data(self, url: str, data: dict) -> None:
        self.rdb.hset(url, mapping=data)

    def get_url_data(self, url: str):
        data = self.rdb.hgetall(url)
        data = {k.decode("utf-8"): v.decode("utf-8") for k, v in data.items()}
        return data

    def update_url_data(self, url: str, data: dict) -> None:
        self.rdb.hset(url, mapping=data)

    def get_all_url(self, url: str) -> dict:
        """Get all cached URL data"""
        all_data = self.rdb.hgetall(url)
        results = {}

        for key, value in all_data.items():
            try:
                key = key.decode("utf-8")
                value = value.decode("utf-8")
            except Exception:
                logger.warning(f"Failed to decode key: {key} or value: {value}")
                pass
            results[key] = value
            # results["status"] = CrawlStatus(results["status"])
        # ensures attrs match URLData, thus the db can store it
        return results

    def close_url(self, url: str) -> None:
        """Close a URL"""
        data = self.get_all_url(url)
        if data:
            self.rdb.delete(url)
            if data["crawl_status"] != CrawlStatus.ERROR.value:
                data["crawl_status"] = CrawlStatus.CLOSED.value
        return data

    def update_status(self, url: str, status: str) -> None:
        """
        Progresses the status of the URL through the crawl pipeline.
        If and error state is passed, the url is closed and removed from the cache.
        """
        if status == "map_site":
            self.rdb.hset(url, "crawl_status", CrawlStatus.FRONTIER.value)
        elif status == "download":
            self.rdb.hset(url, "crawl_status", CrawlStatus.PARSE.value)
        elif status == "parse":
            self.rdb.hset(url, "crawl_status", CrawlStatus.DB.value)
        elif status == "db" or status == "error":
            self.rdb.hset(url, "crawl_status", CrawlStatus.CLOSED.value)
            self.close_url(url)
        else:
            data = self.get_url_data(url)
            if data:
                data["crawl_status"] = status
            self.set_url_data(url, data)


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


class VisitTracker:
    def __init__(self, redis_conn: redis.Redis):
        self.rdb = redis_conn
        self.visited_urls = set()
        self.to_visit = set()

    def get_page_to_visit(self) -> list[str]:
        """Get all frontier seeds for a URL"""
        return self.rdb.lpop("to_visit")

    def add_page_to_visit(self, url: str) -> None:
        """Add a frontier URL to the visit queue"""
        channel = "to_visit"
        # self.rdb.lpush(f"to_visit", url)
        self.rdb.publish(channel, url)
        self.to_visit.add(url)

    def add_page_visited(self, seed: str) -> None:
        """Add a visited seed for a URL"""
        self.visited_urls.add(seed)
        self.rdb.sadd("visited", seed)

    def get_pages_visited(self) -> list[str]:
        """Get all frontier seeds for a URL"""
        return self.rdb.smembers("visited")

    def is_page_visited(self, url: str) -> bool:
        """Check if a page has been visited"""
        resp = self.rdb.sismember("visited", url)
        is_member = bool(resp.decode("utf-8"))
        return is_member
