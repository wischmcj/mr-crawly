from __future__ import annotations

import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import redis
from rq import Queue, Retry, Worker
from rq.command import send_shutdown_command

cwd = os.getcwd()
loc = os.path.dirname(__file__)
print(loc)
sys.path.append(loc)

from parser import extract_urls  # noqa

from cache import CrawlStatus, URLCache, transfer_to_db  # noqa
from config.configuration import get_logger  # noqa
from site_downloader import download_pages  # noqa
from site_mapper import map_site  # noqa

from data import RunTable, UrlTable  # noqa

logger = get_logger(__name__)


def start_worker(queue, redis_conn):
    worker = Worker(connection=redis_conn, queues=[queue])
    worker.work()


BACKOFF_STRATEGY = [10, 30, 60]


class Manager:
    def __init__(
        self,
        seed_url=None,
        max_pages=None,
        parse=None,
        max_workers=1,
        host="localhost",
        port=7777,
        timeout=10,
        retries: int = 3,
    ):
        formatted_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        print("Formatted datetime:", formatted_datetime)
        self.run_id = formatted_datetime
        logger.info(f"Initializing Manager for run_id {self.run_id}")
        self.seed_url = seed_url
        self.max_pages = max_pages
        self.retries = retries
        self.parse = parse
        self.queues = []
        self.timeout = timeout

        self.parse_queue = None
        self.queues = []
        self.cache = URLCache(host=host, port=port, decode_responses=False)
        self.redis_conn = redis.Redis(host=host, port=port, decode_responses=False)
        self._init_dirs()
        self._init_db()
        self._init_queues()
        self._start_workers()

    def _init_dirs(self):
        self.data_dir = os.path.join(os.path.dirname(__file__), "data")
        try:
            os.makedirs(self.data_dir, exist_ok=True)
        except FileExistsError:
            pass
        logger.info("Initializing Directories")
        self.data_dir = os.path.join(os.path.dirname(__file__), f"data/{self.run_id}")
        try:
            os.makedirs(self.data_dir, exist_ok=True)
        except FileExistsError:
            pass
        self.rdb_path = os.path.join(self.data_dir, "data.rdb")

    ## Specify start-up behavior
    def _init_db(self):
        # Initialize databases
        self.run_db = RunTable(self.rdb_path)
        self.url_db = UrlTable(self.rdb_path)
        self.url_db.create_tables()
        self.run_db.start_run(self.seed_url, self.max_pages)
        self.cache.update_url(self.seed_url, "status", CrawlStatus.FRONTIER.value)

    def _init_queues(self):
        logger.info("Initializing Queues")
        # Create different work queues for different tasks
        self.frontier_queue = Queue(connection=self.redis_conn, name="frontier")
        self.site_map_queue = Queue(connection=self.redis_conn, name="site_map")
        self.parse_queue = Queue(connection=self.redis_conn, name="parse")
        self.db_queue = Queue(connection=self.redis_conn, name="db")
        self.queues.append(self.frontier_queue)
        self.queues.append(self.site_map_queue)
        self.queues.append(self.parse_queue)
        self.queues.append(self.db_queue)

    def _start_workers(self):
        executor = ThreadPoolExecutor(max_workers=1)
        for queue in self.queues:
            for i in range(self.max_workers):
                print(f"Started worker {i} for queue: {queue}")
                executor.submit(start_worker, queue, self.redis_conn)

    ## Specify shutdown behavior
    def shutdown(self):
        self.save_cache()
        self._stop_workers()

    def _stop_workers(self):
        logger.info("Stopping workers gracefully")
        worker_stats = {}
        workers = Worker.all(self.redis_conn)
        for worker in workers:
            worker_stats[worker.name] = {
                "successful_jobs": worker.successful_job_count,
                "failed_jobs": worker.failed_job_count,
                "total_working_time": worker.total_working_time,
            }
            send_shutdown_command(self.redis_conn, worker.name)
        workers = Worker.all(self.redis_conn)
        # Wait for workers to close
        while len(workers) > 0:
            time.sleep(1)
            workers = Worker.all(self.redis_conn)
            logger.info(f"Waiting for workers to finish: {len(workers)} remaining")
        logger.info(f"Worker Stats: {worker_stats}")

    def save_cache(self):
        logger.info("Saving cache")
        self.redis_conn.save()
        shutil.copy(loc, self.rdb_path)

    ## Specify runtime behavior
    def enqueue(self, args, req_queue, function, depends_on=None):
        """Add a job to the specified queue"""
        if isinstance(args, str):
            args = (args,)
        logger.debug(f"Enqueuing {args} in {req_queue.name}...")
        # self.wait_for_workers(self.frontier_queue)
        job = req_queue.enqueue(
            function,
            args=args,
            depends_on=depends_on,
            retry=Retry(max=self.retries),
            interval=BACKOFF_STRATEGY,
        )
        return job

    def enqueue_parsing(self, links):
        """Add links to URL extraction queue"""
        logger.debug(f"Enqueuing parsing for {links}")
        self.site_map_queue.enqueue("extract_urls", links)

    def get_queue_sizes(self):
        """Get size of all queues"""
        logger.debug("Getting queue sizes")
        return {
            "frontier": self.frontier_queue.count,
            "site_map": self.site_map_queue.count,
            "parse_page": self.parse_queue.count,
        }

    def process_url(self, seed_url):
        """
        Queues the download, map_site, and parse_page jobs for the given url.
        Returns the download, map_site, and parse_page jobs.
        """
        map_site_task = self.enqueue(seed_url, self.site_map_queue, map_site)  # noqa
        pubsub = self.redis_conn.pubsub()
        pubsub.subscribe(f"{seed_url}:download_needed")

        # Listen for messages
        for message in pubsub.listen():
            if message["type"] == "message":
                curr_url = message["data"].decode()
                download_task = self.enqueue(
                    curr_url, self.frontier_queue, download_pages
                )  # noqa
                parse_task = self.enqueue(
                    (seed_url, curr_url),
                    self.parse_queue,
                    extract_urls,
                    depends_on=map_site_task.id,
                )  # noqa
                db_write_task = self.enqueue(
                    curr_url, self.db_queue, transfer_to_db, depends_on=parse_task.id
                )  # noqa
        return download_task, map_site_task, parse_task, db_write_task


def crawl(seed_url: str):
    """Crawl the given url"""
    manager = Manager()
    r = manager.redis_conn
    workers = Worker.all(r)
    print(f"Workers: {workers}")
    seed_job = manager.enqueue(seed_url, manager.frontier_queue, download_page)  # noqa
    outstanding_jobs = manager.get_queue_sizes()
    while outstanding_jobs["download_html"] > 0:
        time.sleep(1)
        outstanding_jobs = manager.get_queue_sizes()
        print(f"Outstanding jobs: {outstanding_jobs}")
    return manager


if __name__ == "__main__":
    crawl("https://www.google.com/sitemap.xml")
