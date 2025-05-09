from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

import redis
import rq
from config.configuration import get_logger  # noqa
from rq import Queue
from rq.registry import StartedJobRegistry

logger = get_logger(__name__)


class CrawlStatus(Enum):
    """Enum for tracking URL crawl status"""

    SITE_MAP = "site_map"
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
    """`Data` structure for URL metadata"""

    url: str
    content: dict | None = None
    status: CrawlStatus = CrawlStatus.FRONTIER
    run_id: str | None = None
    links: list[str] | None = None
    created_at: str | None = None

    # html: str | None = None
    # download_status: dict | None = None
    # source_url: str | None = None
    # sitemap_frequency: str | None = None
    # sitemap_priority: float | None = None
    # last_modified: str | None = None
    # status: CrawlStatus = CrawlStatus.FRONTIER
    # is_sitemap: bool | None = None
    # is_sitemap_index: bool | None = None
    # kwargs: Dict[str, Any] = field(default_factory=dict, init=False, repr=False, compare=False)

    def __init__(self, a: int, b: str, **kwargs: Any):
        self.a = a
        self.b = b
        self.kwargs = kwargs


class URLCache:
    """Redis-based cache for URL data"""

    def __init__(self, redis_conn: redis.Redis):
        self.rdb = redis_conn
        self.queues = []

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

    def update_content(self, url: str, content, status) -> None:
        """Store URL data in cache"""
        self.rdb.hset(url, "content", content)
        self.rdb.hset(url, "status", status)

    def get_cached_response(self, url: str) -> URLData | None:
        """Retrieve URL data from cache"""
        content, status = None, None
        bcontent = self.rdb.hget(url, "content")
        bstatus = self.rdb.hget(url, "status")
        if bcontent:
            content = bcontent.decode("utf-8")
        if bstatus:
            status = bstatus.decode("utf-8")
        return content, status

    def close_url(self, url: str) -> None:
        """Close a URL"""
        data = self.get_all_url(url)
        if data:
            self.rdb.delete(url)
            if data.status != CrawlStatus.ERROR:
                data.status = CrawlStatus.CLOSED
        return data

    def get_all_url(self, url: str) -> dict[str, URLData]:
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
        result = URLData(**results)
        return result

    def update_status(self, url: str, status: str) -> None:
        """
        Progresses the status of the URL through the crawl pipeline
        """
        if status not in CrawlStatus.__members__:
            raise ValueError(f"Invalid status: {status}")
        if status == "map_site":
            self.rdb.hset(url, "status", CrawlStatus.FRONTIER.value)
        elif status == "download":
            self.rdb.hset(url, "status", CrawlStatus.PARSE.value)
        elif status == "parse":
            self.rdb.hset(url, "status", CrawlStatus.DB.value)
        elif status == "db" or status == "error":
            self.rdb.hset(url, "status", CrawlStatus.CLOSED.value)
            self.close_url(url)
        else:
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


class QueueManager:
    """Redis-based cache for URL data"""

    def __init__(self, redis_conn: redis.Redis, queues_async: bool = True):
        self.rdb = redis_conn
        self.queues = []
        self.registries = []
        self._init_queues(queues_async)
        self._init_registries()
        # self.rdb.flushdb()

    def _init_queues(self, is_async: bool = True):
        logger.info("Initializing Queues")
        # Create different work queues for different tasks
        self.site_map_queue = Queue(
            connection=self.rdb, is_async=is_async, name="site_map"
        )
        self.frontier_queue = Queue(
            connection=self.rdb, is_async=is_async, name="frontier"
        )
        self.parse_queue = Queue(connection=self.rdb, is_async=is_async, name="parse")
        self.queues.append(self.frontier_queue)
        self.queues.append(self.site_map_queue)
        self.queues.append(self.parse_queue)

    def _init_registries(self):
        logger.info("Initializing Registries")
        for queue in self.queues:
            self.registries.append(
                StartedJobRegistry(connection=self.rdb, name=queue.name)
            )

    def _close_queues(self, force: bool = False):
        if force:
            logger.info("Forcefully cancelling jobs")
            self._cancel_all_jobs()
        else:
            logger.info("Gracefully waiting for jobs to finish")
            running = self.get_running_count()
            time_waited = 0
            check_every = 5
            while running > 0 and time_waited < 120:
                time.sleep(check_every)
                running = self.get_running_count()
                time_waited += check_every
                if time_waited > 120:
                    logger.warning(
                        "Timeout waiting for jobs to finish, cancelling jobs"
                    )
                    self._cancel_all_jobs()
        for queue in self.queues:
            queue.close()

    def _cancel_all_jobs(self):
        for queue in self.queues:
            for job in queue.jobs:
                job.cancel()

    def get_running_count(self):
        running = 0
        for registry in self.registries:
            running += registry.get_job_count()
        return running

    def get_redis_conn(self):
        """Get the Redis connection"""
        return self.rdb

    def add_queue(self, queue: rq.Queue) -> None:
        """Add a queue to the cache"""
        self.queues.append(queue)

    def get_queues(self, name: str = None) -> list[rq.Queue]:
        """Get a queue from the cache"""
        if name is not None:
            for queue in self.queues:
                if queue.name == name:
                    return [queue]
        else:
            return self.queues
