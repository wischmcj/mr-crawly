from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass

from cache import URLData
from config.configuration import get_logger

logger = get_logger(__name__)


class DatabaseManager:
    def __init__(self, db_file="data/db.sqlite", logger=None):
        self.conn = sqlite3.connect(db_file)
        self.db_file = db_file
        self._init_db()

    def _init_db(self):
        # Initialize databases
        self.run_db = RunTable(self.conn)
        self.url_db = UrlTable(self.conn)
        self.links_db = LinksTable(self.conn)
        self.sitemap_table = SitemapTable(self.conn)
        self.create_tables()

    def create_tables(self) -> None:
        """Create all database tables"""
        self.url_db.create_table()
        self.links_db.create_table()
        self.sitemap_table.create_table()
        self.run_db.create_table()

    def start_run(self, run_id: str, seed_url: str, max_pages: int) -> int:
        """Start a new crawl run"""
        return self.run_db.start_run(run_id, seed_url, max_pages)

    def complete_run(self, run_id: str, status: str = "completed") -> None:
        """Complete a crawl run"""
        self.run_db.complete_run(run_id, status)

    def store_url(self, url_data: URLData, run_id: str, parent_url: str = None) -> None:
        """Store URL data in database"""
        self.url_db.store_url(url_data, run_id, parent_url)

    def store_links(
        self, seed_url: str, parent_url: str, linked_urls: list[str]
    ) -> None:
        """Store link between URLs"""
        self.links_db.store_links(seed_url, parent_url, linked_urls)

    def store_sitemap(self, url: str, sitemap_data: dict) -> None:
        """Store sitemap data"""
        self.sitemap_table.store_sitemap_data(url, sitemap_data)

    def get_urls_for_seed_url(self, seed_url: str) -> list[dict]:
        """Get all URL records for a given seed URL"""
        return self.url_db.get_urls_for_seed_url(seed_url)

    def get_urls_for_run(self, run_id: str) -> list[dict]:
        """Get all URL records for a given run ID"""
        return self.url_db.get_urls_for_run(run_id)

    def get_links_for_seed_url(self, seed_url: str) -> list[tuple]:
        """Get all link records for a given seed URL"""
        return self.links_db.get_links_for_seed_url(seed_url)

    def get_links_for_parent_url(self, parent_url: str) -> list[tuple]:
        """Get all link records for a given parent URL"""
        return self.links_db.get_links_for_parent_url(parent_url)

    def get_sitemaps_for_seed_url(self, seed_url: str) -> list[dict]:
        """Get all sitemap records for a given seed URL"""
        return self.sitemap_table.get_sitemaps_for_seed_url(seed_url)


class BaseTable:
    def __init__(self, conn, table_name: str = ""):
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.logger = get_logger(__name__)
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
        print(create_string)
        return create_string

    def create_table(self):
        """Create a table if it doesn't exist"""
        create_string = self.build_create_string()
        self.cursor.execute(create_string)
        self.conn.commit()

    def execute_query(self, query: str, params: tuple = ()):
        """Execute a query"""
        self.logger.info(f"Executing query: {query}")
        for i in range(3):
            try:
                self.cursor.execute(query, params)
                self.conn.commit()
                return True
            except sqlite3.OperationalError as e:
                self.logger.error(f"Integrity error: {e}")
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
        return self.cursor.lastrowid

    def complete_run(self, run_id: str, status: str = "completed"):
        """Mark a run as completed and set end time"""
        res = self.execute_query(
            """UPDATE runs SET
                            end_time = datetime('now'),
                            status = ?
                            WHERE run_id = ?;""",
            (run_id, status),
        )
        return res


class UrlTable(BaseTable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = "urls"
        self.columns = [
            "id",
            "seed_url",
            "parent_url",
            "url",
            "content",
            "req_status",
            "crawl_status",
            "run_id",
            "links",
        ]
        self.types = [
            "INTEGER",
            "TEXT",
            "TEXT",
            "TEXT",
            "BLOB",
            "TEXT",
            "INTEGER",
            "INTEGER",
            "TEXT",
        ]
        self.primary_key = "id"
        self.unique_keys = ["id", "url"]

    def get_urls_for_run(self, run_id: str) -> list[dict]:
        """Get all URL records for a given run ID"""
        res = self.execute_query(
            """SELECT id, seed_url, parent_url, url, content, req_status, crawl_status, run_id, links
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
            """SELECT id, seed_url, parent_url, url, content, req_status, crawl_status, run_id, links
               FROM urls
               WHERE seed_url = ?""",
            (seed_url,),
        )
        if res:
            urls = self.cursor.fetchall()
            return [dict(zip(self.columns, url)) for url in urls]
        else:
            raise Exception("Failed to get URLs for seed URL")

    def store_url(self, url_data: dict, run_id: str, seed_url: str):
        """Store URL and its HTML content"""
        url = url_data["url"]
        content = url_data["content"]
        req_status = url_data["req_status"]
        crawl_status = url_data["crawl_status"]
        parent_url = url_data["parent_url"]
        try:
            res = self.execute_query(
                "INSERT INTO urls (seed_url, parent_url, url, content, req_status, crawl_status, run_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (seed_url, parent_url, url, content, req_status, crawl_status, run_id),
            )
        except sqlite3.IntegrityError:
            # Update if entry already exists
            self.execute_query(
                "UPDATE urls SET content = ?, req_status = ?, crawl_status = ? WHERE url = ? AND run_id = ?",
                (content, req_status, crawl_status, url, run_id),
            )
        if res:
            return self.cursor.lastrowid
        else:
            raise Exception("Failed to store URL")


class LinksTable(BaseTable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = "links"
        self.columns = ["id", "seed_url", "parent_url", "linked_url"]
        self.types = ["INTEGER", "TEXT", "TEXT", "TEXT"]
        self.primary_key = "id"

    def get_links_for_seed_url(self, seed_url: str) -> list[tuple]:
        """Get all link records where this URL is the source"""
        res = self.execute_query(
            """SELECT seed_url, parent_url, linked_url
               FROM links
               WHERE seed_url = ?""",
            (seed_url,),
        )
        if res:
            return self.cursor.fetchall()
        else:
            raise Exception("Failed to get links for seed URL")

    def get_links_for_parent_url(self, parent_url: str) -> list[tuple]:
        """Get all link records where this URL is the source"""
        res = self.execute_query(
            """SELECT parent_url, linked_url
               FROM links
               WHERE parent_url = ?""",
            (parent_url,),
        )
        if res:
            return self.cursor.fetchall()
        else:
            raise Exception("Failed to get links for parent URL")

    def store_links(self, seed_url: str, parent_url: str, linked_urls: list[str]):
        """Store multiple links from a source URL"""
        try:
            # Create list of tuples for executemany
            links_data = list(
                tuple((seed_url, parent_url, linked_url)) for linked_url in linked_urls
            )

            self.conn.executemany(
                """INSERT INTO links (seed_url, parent_url, linked_url)
                   VALUES (?, ?, ?)""",
                links_data,
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            # Log error but continue - some duplicates are expected
            pass


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

    def store_sitemap_data(self, sitemap_details: dict):
        """Store sitemap metadata"""
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
