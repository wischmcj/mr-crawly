from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from threading import Event

from config.configuration import get_logger
from helper_classes import BaseListener

logger = get_logger("data")
exit_event = Event()


class UrlBulkWriter(BaseListener):
    def __init__(self, pubsub, db_file, batch_size=5):
        """Initialize with a Redis pubsub object"""
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.pubsub = pubsub
        self.batch_size = batch_size
        self.urls_to_write = []
        self.url_db = UrlTable(self.conn)
        self.url_db.create_table()
        self.running: bool = True

    def store_url(self, url_data: dict) -> None:
        """Store URL data in database"""
        self.urls_to_write.append(url_data)
        logger.debug(f"Currently {len(self.urls_to_write)} urls to write")
        if len(self.urls_to_write) > self.batch_size:
            self.url_db.store_urls(self.urls_to_write)
            self.urls_to_write = []

    def get_urls_for_seed_url(self, seed_url: str) -> list[dict]:
        """Get all URL records for a given seed URL"""
        return self.url_db.get_urls_for_seed_url(seed_url)

    def get_urls_for_run(self, run_id: str) -> list[dict]:
        """Get all URL records for a given run ID"""
        return self.url_db.get_urls_for_run(run_id)

    def flush_urls(self) -> None:
        """Flush the URLs to the database"""
        logger.debug("Flushing URLs to database")
        self.url_db.store_urls(self.urls_to_write)
        self.urls_to_write = []
        logger.debug("Closing connection to database")

    def handle_message(self):
        while self.running:
            for message in self.pubsub.listen():
                logger.debug(f"Received message: {message}")
                if message["type"] == "message":
                    if message["data"] == b"exit":
                        self.running = False
                        break
                    else:
                        url_data = json.loads(message.get("data", ""))
                        self.store_url(url_data)
        self.flush_urls()
        exit_event.set()


class DatabaseManager:
    def __init__(self, url_pubsub, db_file="data/db.sqlite"):
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.db_file = db_file
        self.url_pubsub = url_pubsub
        self._init_db()
        self.urls_to_write = []
        self.write_every = 10

    def _init_db(self):
        # Initialize databases
        self.run_db = RunTable(self.conn)
        self.sitemap_table = SitemapTable(self.conn)
        self.create_tables()
        self.listeners = []
        self.add_listener(self.url_pubsub, UrlBulkWriter, (self.db_file,))

    def shutdown(self):
        """Shutdown the database manager"""
        logger.info("Shutting down database manager")
        self.conn.close()

    def add_listener(self, pubsub, listener_cls, args=()):
        logger.info(f"Subscribed to {pubsub}. Waiting for messages...")
        handler = listener_cls(pubsub, *args)
        handler.start()
        self.listeners.append(handler)

    def create_tables(self) -> None:
        """Create all database tables"""
        self.sitemap_table.create_table()
        self.run_db.create_table()

    def start_run(self, run_id: str, seed_url: str, max_pages: int) -> int:
        """Start a new crawl run"""
        return self.run_db.start_run(run_id, seed_url, max_pages)

    def complete_run(self, run_id: str) -> None:
        """Complete a crawl run"""
        self.run_db.complete_run(run_id)

    def store_links(
        self, seed_url: str, parent_url: str, linked_urls: list[str]
    ) -> None:
        """Store link between URLs"""
        self.links_db.store_links(seed_url, parent_url, linked_urls)

    def store_sitemap(self, url: str, sitemap_data: dict) -> None:
        """Store sitemap data"""
        self.sitemap_table.store_sitemap_data(url, sitemap_data)

    def get_sitemaps_for_seed_url(self, seed_url: str) -> list[dict]:
        """Get all sitemap records for a given seed URL"""
        return self.sitemap_table.get_sitemaps_for_seed_url(seed_url)


class BaseTable:
    def __init__(self, conn, table_name: str = ""):
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.table_name = table_name
        self.columns = []
        self.types = []
        self.primary_key = ""
        self.unique_keys = []

    def build_create_string(self):
        """Build a create string for a table"""
        cols_to_types = dict(zip(self.columns, self.types))

        if len(self.unique_keys) == 0:
            self.unique_keys = [self.primary_key]
        unique_cols = ", ".join(self.unique_keys)
        pk = self.primary_key
        pk_row = {pk: f"{cols_to_types[pk]} PRIMARY KEY AUTOINCREMENT"}
        cols_to_types.update(pk_row)
        cols_w_types = [f"{col} {ctype}" for col, ctype in cols_to_types.items()]

        create_cols_string = ", ".join(cols_w_types)
        create_string = f"""CREATE TABLE IF NOT EXISTS {self.table_name} (
                                    {create_cols_string},
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                    UNIQUE({unique_cols})
                                )"""
        logger.debug(create_string)
        return create_string

    def create_table(self):
        """Create a table if it doesn't exist"""
        create_string = self.build_create_string()
        self.cursor.execute(create_string)
        self.conn.commit()

    def execute_query(self, query: str, params: tuple = ()):
        """Execute a query"""
        logger.debug(f"Executing query: {query}")
        for i in range(3):
            try:
                self.cursor.execute(query, params)
                self.conn.commit()
                return True
            except sqlite3.OperationalError as e:
                logger.error(f"Integrity error: {e}")
                self.conn.rollback()
                time.sleep(1)
        return False


@dataclass
class Run:
    run_id: str
    seed_url: str
    start_time: str
    max_pages: int
    end_time: str | None = None


class RunTable(BaseTable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = "runs"
        self.columns = [
            "id",
            "run_id",
            "seed_url",
            "start_time",
            "max_pages",
            "end_time",
        ]
        self.types = ["INTEGER", "TEXT", "TEXT", "DATETIME", "INTEGER", "DATETIME"]
        self.primary_key = "id"
        self.unique_keys = ["id", "run_id"]

    def start_run(
        self,
        run_id: str,
        seed_url: str,
        max_pages: int,
    ):
        """Create a new run record when crawler starts"""
        self.execute_query(
            """INSERT INTO runs (run_id, seed_url, start_time, max_pages)
                              VALUES (?, ?, datetime('now'), ?);""",
            (run_id, seed_url, max_pages),
        )
        self.conn.commit()
        return self.cursor.lastrowid

    def complete_run(self, run_id: str):
        """Mark a run as completed and set end time"""
        logger.info(f"Completing run {run_id}")
        res = self.execute_query(
            """UPDATE runs SET
                            end_time = datetime('now')
                            WHERE run_id = ?;""",
            (run_id,),
        )
        self.conn.commit()
        return res


class UrlTable(BaseTable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = "urls"
        self.columns = [
            "id",
            "seed_url",
            "url",
            "content",
            "req_status",
            "crawl_status",
            "run_id",
            "linked_urls",
        ]
        self.types = [
            "INTEGER",
            "TEXT",
            "TEXT",
            "BLOB",
            "TEXT",
            "TEXT",
            "TEXT",
            "BLOB",
        ]
        self.primary_key = "id"
        self.unique_keys = ["id"]

    def get_urls_for_run(self, run_id: str) -> list[dict]:
        """Get all URL records for a given run ID"""
        res = self.execute_query(
            """SELECT id, seed_url, url, content, req_status, crawl_status, run_id, linked_urls
               FROM urls
               WHERE run_id = ?""",
            (run_id,),
        )
        if res:
            urls = self.cursor.fetchall()
            return [dict(zip(self.columns, url)) for url in urls]
        else:
            raise Exception("Failed to get URLs for run")

    def get_urls_for_seed_url(self, seed_url: str) -> list[dict]:
        """Get all URL records for a given seed URL"""
        res = self.execute_query(
            """SELECT id, seed_url, url, content, req_status, crawl_status, run_id, linked_urls
               FROM urls
               WHERE seed_url = ?""",
            (seed_url,),
        )
        if res:
            urls = self.cursor.fetchall()
            return [dict(zip(self.columns, url)) for url in urls]
        else:
            raise Exception("Failed to get URLs for seed URL")

    def store_urls(self, url_data: list[dict]):
        """Store URL and its HTML content"""
        to_write = []
        if len(url_data) > 0:
            for row in url_data:
                url = row.get("url", "")
                content = row.get("content", "")
                req_status = row.get("req_status", "")
                crawl_status = row.get("crawl_status", "")
                seed_url = row.get("seed_url", "")
                run_id = row.get("run_id", "")
                linked_urls = json.dumps(row.get("linked_urls", []))
                to_write.append(
                    [
                        seed_url,
                        url,
                        content,
                        req_status,
                        crawl_status,
                        run_id,
                        linked_urls,
                    ]
                )
            insert_query = "INSERT INTO urls (seed_url, url, content, req_status, crawl_status, run_id, linked_urls) VALUES "
            val_lists = [
                "','".join([str(x) if x else "" for x in row]) for row in to_write
            ]
            vals = ",".join(f"('{val_list}')" for val_list in val_lists)
            query = insert_query + vals
            logger.info("running query", query)
            _ = self.execute_query(
                query,
            )
            return self.cursor.lastrowid
        else:
            logger.info("No URLs to store")
            return None


class SitemapTable(BaseTable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = "sitemaps"
        self.columns = [
            "id",
            "run_id",
            "seed_url",
            "url",
            "index_url",
            "loc",
            "priority",
            "frequency",
            "modified",
            "status",
        ]
        self.types = [
            "INTEGER",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
        ]
        self.primary_key = "id"

    def get_sitemaps_for_seed_url(self, seed_url: str) -> list[dict]:
        """Get all sitemap records for a given seed URL"""
        res = self.execute_query(
            """SELECT run_id, seed_url, url, index_url, loc, priority, frequency, modified, status
               FROM sitemaps
               WHERE seed_url = ?""",
            (seed_url,),
        )
        if res:
            sitemaps = self.cursor.fetchall()
            return list(
                dict(
                    zip(
                        [
                            "run_id",
                            "seed_url",
                            "url",
                            "index_url",
                            "loc",
                            "priority",
                            "frequency",
                            "modified",
                            "status",
                        ],
                        sitemap,
                    )
                )
                for sitemap in sitemaps
            )
        else:
            raise Exception("Failed to get sitemaps for seed URL")

    def store_sitemap_data(self, sitemap_details: dict, run_id: str, seed_url: str):
        """Store sitemap metadata"""
        sitemap_details["run_id"] = run_id
        sitemap_details["seed_url"] = seed_url
        try:
            self.conn.execute(
                """
                INSERT INTO sitemaps
                (run_id, seed_url, url, index_url, loc, priority, frequency, modified, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sitemap_details["run_id"],
                    sitemap_details["seed_url"],
                    sitemap_details.get("url", None),
                    sitemap_details.get("index_url", None),
                    sitemap_details.get("loc", None),
                    sitemap_details.get("priority", None),
                    sitemap_details.get("frequency", None),
                    sitemap_details.get("modified", None),
                    sitemap_details.get("status", None),
                ),
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            # Update if entry already exists
            self.conn.execute(
                """
                UPDATE sitemaps
                SET loc = ?, priority = ?, frequency = ?, modified = ?, status = ?
                WHERE parent_url = ?
                """,
                (
                    sitemap_details.get("loc", None),
                    sitemap_details.get("priority", None),
                    sitemap_details.get("frequency", None),
                    sitemap_details.get("modified", None),
                    sitemap_details.get("status", None),
                    sitemap_details.get("parent_url", None),
                ),
            )
            self.conn.commit()
