from __future__ import annotations

import json
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import redis
from rq import Retry, Worker
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

from data import LinksTable, RunTable, SitemapTable, UrlTable  # noqa

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
        self._init_dirs()
        self._init_db()
        self._start_workers()

    def _init_dirs(self):
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
        try:
            print("Making directory" + self.data_dir)
            os.makedirs(self.data_dir, exist_ok=True)
        except FileExistsError:
            print("Directory already exists")
            pass
        logger.info("Initializing Directories")
        self.data_dir = os.path.join(self.data_dir, f"{self.run_id}")
        try:
            print("Making directory" + self.data_dir)
            os.makedirs(self.data_dir, exist_ok=True)
        except FileExistsError:
            print("Directory already exists")
            pass
        self.rdb_path = os.path.join(self.data_dir, "data.rdb")

    ## Specify start-up behavior
    def _init_db(self):
        # Initialize databases
        print(self.data_dir)
        self.run_db = RunTable(self.data_dir + "/sqlite.db")
        self.url_db = UrlTable(self.data_dir + "/sqlite.db")
        self.links_db = LinksTable(self.data_dir + "/sqlite.db")
        self.sitemap_table = SitemapTable(self.data_dir + "/sqlite.db")
        self.url_db.create_tables()
        self.run_db.start_run(self.seed_url, self.max_pages)

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
        self._stop_workers()
        self.run_db.complete_run(self.run_id)
        self.qmanager._close_queues(force=force)
        self.save_cache()

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

    ## Specify runtime behavior
    def enqueue(
        self,
        args,
        req_queue,
        function,
        on_success_callback,
        on_failure_callback,
        depends_on=None,
    ):
        """Add a job to the specified queue, with the
        optionally specified dependencies and callbacks"""
        if isinstance(args, str):
            args = (args,)
        logger.info(
            f"Enqueuing {args} in {req_queue.name}: {on_success_callback} {on_failure_callback}"
        )
        job = req_queue.enqueue(
            function,
            args=args,
            on_success=on_success_callback,
            on_failure=on_failure_callback,
            depends_on=depends_on,
            retry=Retry(max=self.retries),
            interval=BACKOFF_STRATEGY,
        )
        return job

    def enqueue_page(self, seed_url, curr_url):
        """
        This represents the crawling of a single page.
        1. Download the content, if possible, cache to redis
        2. Extract urls from the page, return them
        3. Enqueue the urls for parsing
        """
        logger.debug(f"Enqueuing download for {curr_url}")
        download_task = self.enqueue(
            (seed_url, curr_url), self.qmanager.frontier_queue, download_page
        )  # noqa
        parse_task = self.enqueue(
            (seed_url, curr_url),
            self.qmanager.parse_queue,
            extract_urls,
            depends_on=download_task.id,
            on_success=self.on_parse_success,
            on_failure=self.on_parse_failure,
        )  # noqa

        return download_task, parse_task

    #   General on end functions to update status/save data
    def on_success(self, job, connection, result, func_name):
        """Callback for when a job succeeds"""
        url = job.args[0]
        logger.info(f"{func_name} job {job.id} succeeded")
        self.cache.update_status(url, "status", func_name)
        return url, result

    def on_failure(self, job, connection, type, value, traceback, func_name):
        """Callback for when a job succeeds"""
        url = job.args[0]
        logger.info(f"{func_name} job {job.id} failed")
        if func_name != "map_site":
            url_data = self.cache.update_status(url, "status", "error")
            if url_data:
                self.url_db.store_url_content(url_data, self.run_id)
                self.url_db.store_url(url_data)
        return url

    # Map site specific on end functions
    def on_map_success(self, job, connection, result):
        url, result = self.on_success(job, connection, result, "map_site")
        """Callback for when a site mapping job succeeds"""
        root_sitemap_url, sitemap_indicies, sitemap_details = result
        self.logger.info(
            "Writing sitemap data to sqlite and the top level urls to rdb cache"
        )
        with open(f"{self.data_dir}/sitemap_indexes.json", "w") as f:
            json.dump(sitemap_indicies, f, default=str, indent=4)

        for detail in sitemap_details:
            self.sitemap_table.store_sitemap_data(detail)
            url = detail.get("loc")
            self.enqueue_page(self.seed_url, url)

    def on_map_failure(self, job, connection, type, value, traceback):
        """Callback for when a site mapping job fails"""
        url, result = self.on_failure(
            job, connection, type, value, traceback, "map_site"
        )
        logger.info("Utilizing fallback to seed_url links")
        self.enqueue_page(self.seed_url, self.seed_url)

    # Download specific on end functions
    def on_download_success(self, job, connection, queue, result):
        """
        When a download success, progress the urls status,
            and enqueue the page for parsing.
        """
        url, result = self.on_success(job, connection, result, "download")
        self.cache.update_content(url, result[0], result[1])
        self.visited_urls.add(url)
        if len(self.visited_urls) >= self.max_pages:
            # Allow submitted tasks to finish, but shutdown workers
            logger.info(f"Downloaded {len(self.visited_urls)} urls")
            logger.info("Shutting down...")
            self.shutdown()

    def on_download_failure(self, job, connection, type, value, traceback):
        """Callback for when a download job fails"""
        url, result = self.on_failure(job, connection, type, value, traceback, "error")

    # Parse specific on end functions
    def on_parse_success(self, job, connection, result):
        """Callback for when a parse job succeeds"""
        logger.info(f"Parse job {job.id} succeeded")
        seed_url, current_url, new_links = result
        self.links_db.store_links(seed_url, current_url, new_links)
        for link in new_links:
            self.enqueue_page(seed_url, link)

    def on_parse_failure(self, job, connection, type, value, traceback):
        """Callback for when a download job fails"""
        url, result = self.on_failure(job, connection, type, value, traceback, "error")

    def process_url(self, seed_url):
        """
        Queues the download, map_site, and parse_page jobs for the given url.
        Returns the download, map_site, and parse_page jobs.
        """
        _ = self.enqueue(
            args=seed_url,
            req_queue=self.qmanager.site_map_queue,
            function=map_site,
            on_success_callback=self.on_map_success,
            on_failure_callback=self.on_map_failure,
        )  # noqa
