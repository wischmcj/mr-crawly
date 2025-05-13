from __future__ import annotations

import os
import shutil
import sys
from datetime import datetime

import redis
from utils import create_dir

from data import DatabaseManager

cwd = os.getcwd()
loc = os.path.dirname(__file__)
print(loc)
sys.path.append(loc)

from cache import CrawlTracker, URLCache  # noqa
from config.configuration import get_logger  # noqa

logger = get_logger("main")


class Manager:
    def __init__(
        self,
        seed_url=None,
        max_pages=None,
        host="localhost",
        port=7777,
        retries: int = 3,
        debug: bool = False,
        db_file="sqlite.db",
        rdb_file="data.rdb",
        run_id=None,
    ):
        if run_id is None:
            formatted_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
            print("Formatted datetime:", formatted_datetime)
            self.run_id = formatted_datetime
        else:
            self.run_id = run_id
        logger.info(f"Initializing Manager for run_id {self.run_id}")
        self.seed_url = seed_url
        self.max_pages = max_pages
        self.retries = retries
        self.is_async = not debug
        self.db_file = db_file
        self.rdb_file = rdb_file
        self.rdb = redis.Redis(host=host, port=port, decode_responses=False)
        self._init_dirs()
        self._init_pubsub()
        self._init_db()
        self._init_cache()

        self.visited_urls = set()
        self.to_visit = set()
        self.listeners = []

    def get_run_data(self):
        data = {}
        data["run_id"] = self.run_id
        data["seed_url"] = self.seed_url
        data["max_pages"] = self.max_pages
        data["retries"] = self.retries
        data["is_async"] = self.is_async
        return data

    def _init_dirs(self):
        self.data_dir = os.path.join(os.path.dirname(__file__), "data")
        create_dir(self.data_dir, exist_ok=True)
        self.data_dir = os.path.join(self.data_dir, f"{self.run_id}")
        create_dir(self.data_dir, exist_ok=True)
        self.rdb_path = os.path.join(self.data_dir, self.rdb_file)
        self.sqlite_path = os.path.join(self.data_dir, self.db_file)

    def _init_pubsub(self):
        url_channel = "db"
        # Initialize databases
        url_pubsub = self.rdb.pubsub()
        url_pubsub.subscribe(url_channel)
        self.url_pubsub = url_pubsub
        return url_pubsub

    def _init_db(self):
        # Initialize databases
        print(self.data_dir)
        self.db_manager = DatabaseManager(self.url_pubsub, self.sqlite_path)

    def _init_cache(self):
        self.cache = URLCache(self.rdb)
        self.crawl_tracker = CrawlTracker(manager=self, url_pubsub=self.url_pubsub)

    def shutdown(self):
        """Shutdown the manager"""
        logger.info("Shutting down manager")
        self.db_manager.shutdown()
        self.url_pubsub.close()

    def set_seed_url(self, seed_url: str):
        self.seed_url = seed_url
        self.crawl_tracker.seed_url = seed_url

    def set_max_pages(self, max_pages: int):
        self.max_pages = max_pages
        self.crawl_tracker.max_pages = max_pages

    def save_cache(self):
        logger.info("Saving cache")
        self.rdb.save()
        shutil.copy(os.path.dirname(loc) + "/dump.rdb", self.rdb_path)
