from __future__ import annotations

import json
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import redis
from rq import Callback, Retry, Worker
from rq.command import send_shutdown_command

cwd = os.getcwd()
loc = os.path.dirname(__file__)
print(loc)
sys.path.append(loc)

from parser import extract_urls  # noqa

from cache import CrawlStatus, QueueManager, URLCache  # noqa
from config.configuration import get_logger  # noqa
from site_downloader import download_page  # noqa
from site_mapper import map_site  # noqa

from data import RunTable, UrlTable  # noqa
from mr_crawly.data import LinksTable  # noqa

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
        num_workers=1,
        host="localhost",
        port=7777,
        retries: int = 3,
        debug: bool = False,
    ):
        formatted_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        print("Formatted datetime:", formatted_datetime)
        self.run_id = formatted_datetime
        logger.info(f"Initializing Manager for run_id {self.run_id}")
        self.seed_url = seed_url
        self.max_pages = max_pages
        self.retries = retries
        self.is_async = not debug
        self.num_workers = num_workers

        self.visited_urls = set()
        self.queues = []
        self.redis_conn = redis.Redis(host=host, port=port, decode_responses=False)
        self.cache = URLCache(self.redis_conn)
        self.qmanager = QueueManager(self.redis_conn, self.is_async)
        self.links_db = LinksTable("sqlite.db")
        self._init_dirs()
        self._init_db()
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
        # self.cache.update_url(self.seed_url, "status", CrawlStatus.FRONTIER.value)

    def _start_workers(self):
        """
        Start rq workers for each queue.
        Workers can be accessed via our redis connection object, so
        we don't save pointers to the workers in the manager.
        """
        queues = self.qmanager.get_queues()
        executor = ThreadPoolExecutor(max_workers=1)
        for queue in queues:
            for i in range(self.num_workers):
                print(f"Started worker {i} for queue: {queue}")
                executor.submit(start_worker, queue, self.redis_conn)

    ## Specify shutdown behavior
    def shutdown(self, force: bool = False):
        self.save_cache()
        self.run_db.complete_run(self.run_id)
        self.qmanager._close_queues(force=force)
        self._stop_workers()

    def _flush_db(self):
        """Flush the database"""
        self.redis_conn.flushdb()

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

    def on_download_success(self, job, connection, queue, result):
        """Callback for when a download job succeeds"""
        url = job.args[0]
        self.visited_urls.add(url)
        logger.info(f"Download job {url} succeeded")

    ## Specify runtime behavior
    def enqueue(self, args, req_queue, function, depends_on=None, on_success=None):
        """Add a job to the specified queue"""
        if isinstance(args, str):
            args = (args,)
        logger.debug(f"Enqueuing {args} in {req_queue.name}...")
        # self.wait_for_workers(self.frontier_queue)
        job = req_queue.enqueue(
            function,
            args=args,
            on_success=Callback(on_success),
            depends_on=depends_on,
            retry=Retry(max=self.retries),
            interval=BACKOFF_STRATEGY,
        )
        return job

    def enqueue_page(self, seed_url, curr_url):
        """Add a download job to the queue"""
        logger.debug(f"Enqueuing download for {curr_url}")
        download_task = self.enqueue(
            (seed_url, curr_url), self.frontier_queue, download_page
        )  # noqa
        parse_task = self.enqueue(
            (seed_url, curr_url),
            self.parse_queue,
            extract_urls,
            depends_on=download_task.id,
            on_success=self.on_parse_success,
        )  # noqa

        return download_task, parse_task

    def on_parse_success(self, job, connection, result):
        """Callback for when a parse job succeeds"""
        logger.info(f"Parse job {job.id} succeeded")
        seed_url, current_url, new_links = result
        self.links_db.store_links(seed_url, current_url, new_links)
        for link in new_links:
            self.enqueue_page(seed_url, link)
        url_data = self.cache.close_url(current_url)
        if url_data:
            self.url_db.store_url_content(url_data, self.run_id)
            self.url_db.store_url(url_data)

    def on_map_success(self, job, connection, result):
        """Callback for when a site mapping job succeeds"""
        logger.info(f"Parse job {job.id} succeeded")
        root_sitemap_url, sitemap_indicies, sitemap_details = result
        self.cache.update_url(root_sitemap_url, "status", CrawlStatus.SITE_MAP.value)

        self.logger.info(
            "Writing sitemap data to sqlite and the top level urls to rdb cache"
        )
        for detail in self.sitemap_details:
            self.sitemap_table.store_sitemap_data(detail)
            url = detail.get("loc")
            try:
                fseed = detail.get("loc")
                ## Save these as frontier seeds
                self.cache.add_frontier_seed(url, fseed)
            except Exception:
                pass

    def on_map_failure(self, job, connection, type, value, traceback):
        """Callback for when a site mapping job fails"""
        logger.info(f"Error Mapping site: {value}")
        logger.info("Utilizing fallback to seed_url links")
        self.enqueue_page(self.seed_url, self.seed_url)

        with open(f"{self.data_dir}/sitemap_indexes.json", "w") as f:
            json.dump(self.sitemap_indexes, f, default=str, indent=4)

    def process_url(self, seed_url):
        """
        Queues the download, map_site, and parse_page jobs for the given url.
        Returns the download, map_site, and parse_page jobs.
        """
        _ = self.enqueue(
            seed_url, self.site_map_queue, map_site, on_success=self.on_map_success
        )  # noqa
        pubsub = self.redis_conn.pubsub()
        pubsub.subscribe(f"{seed_url}:download_needed")

        # Listen for messages
        for message in pubsub.listen():
            if message["type"] == "message":
                curr_url = message["data"].decode()
                self.enqueue_page(seed_url, curr_url)


def crawl(seed_url: str, debug: bool = True):
    """Crawl the given url"""
    manager = Manager()
    manager.process_url(seed_url)

    get_running_count = manager.qmanager.get_running_count()
    while get_running_count > 0:
        logger.info(f"Waiting for {get_running_count} jobs to finish")
        time.sleep(10)
        get_running_count = manager.qmanager.get_running_count()

    seed_job = manager.enqueue(seed_url, manager.frontier_queue, download_page)  # noqa
    outstanding_jobs = manager.get_queue_sizes()
    while outstanding_jobs["download_html"] > 0:
        time.sleep(1)
        outstanding_jobs = manager.get_queue_sizes()
        print(f"Outstanding jobs: {outstanding_jobs}")
    return manager


if __name__ == "__main__":
    crawl("https://www.google.com/sitemap.xml")
