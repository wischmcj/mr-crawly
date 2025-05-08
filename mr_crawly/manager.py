from __future__ import annotations

import os

import redis
from rq import Queue, Worker

from mr_crawly.data import EventTable, RunTable, UrlTable


class Manager:
    def __init__(
        self,
        host="localhost",
        port=7777,
        seed_url=None,
        max_pages=None,
        delay=None,
        parse=None,
        max_workers=3,
    ):
        self.seed_url = seed_url
        self.max_pages = max_pages
        self.delay = delay
        self.parse = parse
        self.max_workers = max_workers

        # Initialize Redis connection
        self.redis_conn = redis.Redis(host=host, port=port, decode_responses=True)
        self._init_db()
        self._init_queues()
        self._start_workers()

    def _init_db(self):
        # Set up database paths
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
        os.makedirs(data_dir, exist_ok=True)
        db_path = os.path.join(data_dir, "sqlite.db")

        # Initialize databases
        self.run_db = RunTable(db_path)
        self.event_db = EventTable(db_path)
        self.url_db = UrlTable(db_path)

    def _init_queues(self):
        # Create different work queues for different tasks
        self.download_queue = Queue(connection=self.redis_conn, name="download_html")
        self.site_map_queue = Queue(connection=self.redis_conn, name="site_map")
        self.parse_queue = Queue(connection=self.redis_conn, name="parse_page")

    def _start_workers(self):
        for queue in self.queues:
            for i in range(self.num_workers):
                worker = Worker(
                    queue, connection=self.redis_conn, name=f"{queue.name}-{i}"
                )
                self.workers.append(worker)
                worker.start()

    def _stop_workers(self):
        worker_stats = {}
        for worker in self.workers:
            worker.stop()
            worker_stats[worker.name] = {
                "successful_jobs": worker.successful_job_count,
                "failed_jobs": worker.failed_job_count,
                "total_working_time": worker.total_working_time,
            }

    def _cleanup(self):
        for worker in self.workers:
            worker.join()
        self.redis_conn.close()

    def get_workers(self):
        workers = Worker.all(connection=self.redis_conn)
        return workers

    def enqueue_download(self, url):
        """Add URL to HTML download queue"""
        self.download_queue.enqueue("download_html", url)

    def enqueue_site_mapping(self, html):
        """Add HTML to link aggregation queue"""
        self.site_map_queue.enqueue("aggregate_links", html)

    def enqueue_parsing(self, links):
        """Add links to URL extraction queue"""
        self.site_map_queue.enqueue("extract_urls", links)

    def get_queue_sizes(self):
        """Get size of all queues"""
        return {
            "download_html": self.download_queue.count,
            "site_map": self.site_map_queue.count,
            "parse_page": self.parse_queue.count,
        }

    def request_page(self, url: str) -> tuple[str, int]:
        """Request the html for the given url from manager
        returns html and status code"""
        raise NotImplementedError("This method should be implemented later")

    def get_page(self, url: str):
        """check for the url in the db and, if not available,
        put a request for the url in the downloader queue"""
        # Check if page exists in database
        page = self.url_db.get_page(url)
        if page:
            return page

        # If not found, add to download queue
        self.html_queue.enqueue("download_html", url)
        return None

    def flush_all(self):
        """Clear all queues"""
        self.html_queue.empty()
        self.link_queue.empty()
        self.extract_queue.empty()
        self.parse_queue.empty()
