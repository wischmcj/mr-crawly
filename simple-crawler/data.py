from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from cache import URLData
from config.configuration import get_logger

logger = get_logger(__name__)


class DatabaseManager:
    def __init__(self, db_file, logger=None):
        self.db_file = db_file
        self._init_db()

    def _init_db(self):
        # Initialize databases
        print(self.data_dir)
        self.run_db = RunTable(self.db_file)
        self.url_db = UrlTable(self.db_file)
        self.links_db = LinksTable(self.db_file)
        self.sitemap_table = SitemapTable(self.db_file)
        self.url_db.create_tables()

    def start_run(self, seed_url: str, max_pages: int) -> int:
        """Start a new crawl run"""
        return self.run_db.start_run(seed_url, max_pages)

    def complete_run(self, run_id: int, status: str = "completed") -> None:
        """Complete a crawl run"""
        self.run_db.complete_run(run_id, status)

    def create_tables(self) -> None:
        """Create all database tables"""
        self.url_db.create_table()
        self.links_db.create_table()
        self.sitemap_table.create_table()
        self.run_db.create_table()

    def store_url(self, url_data: URLData, run_id: int, parent_url: str = None) -> None:
        """Store URL data in database"""
        self.url_db.store_url(url_data, run_id, parent_url)

    def store_link(self, source_url: str, target_url: str, run_id: int) -> None:
        """Store link between URLs"""
        self.links_db.store_link(source_url, target_url, run_id)

    def store_sitemap(self, url: str, sitemap_data: dict, run_id: int) -> None:
        """Store sitemap data"""
        self.sitemap_table.store_sitemap(url, sitemap_data, run_id)

    def get_urls_for_run(self, run_id: int) -> list[URLData]:
        """Get all URLs for a run"""
        return self.url_db.get_urls_for_run(run_id)

    def get_links_for_run(self, run_id: int) -> list[tuple[str, str]]:
        """Get all links for a run"""
        return self.links_db.get_links_for_run(run_id)

    def get_sitemaps_for_run(self, run_id: int) -> list[dict]:
        """Get all sitemaps for a run"""
        return self.sitemap_table.get_sitemaps_for_run(run_id)


class BaseTable:
    def __init__(self, connection, table_name: str = ""):
        self.connection = connection
        self.cursor = self.connection.cursor()
        self.logger = get_logger(__name__)
        self.table_name = table_name
        self.columns = []
        self.types = []
        self.primary_key = ""

    def build_create_string(self):
        """Build a create string for a table"""
        cols_to_types = dict(zip(self.columns, self.types))

        pk = self.primary_key
        pk_row = {pk: f"{cols_to_types[pk]} PRIMARY KEY AUTOINCREMENT"}
        cols_to_types.update(pk_row)
        cols_w_types = [f"{col} {ctype}" for col, ctype in cols_to_types.items()]

        create_cols_string = ", ".join(cols_w_types.items())
        create_string = f"""CREATE TABLE IF NOT EXISTS {self.table_name} (
                                    {create_cols_string},
                                    UNIQUE({pk})
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                )"""
        return create_string

    def create_table(self):
        """Create a table if it doesn't exist"""
        create_string = self.build_create_string()
        self.cursor.execute(create_string)
        self.connection.commit()


@dataclass
class Run:
    run_id: int
    seed_url: str
    start_time: str
    max_pages: int
    end_time: str | None = None


class RunTable(BaseTable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = ["run_id", "seed_url", "start_time", "max_pages", "end_time"]
        self.types = ["INTEGER", "TEXT", "TEXT", "INTEGER", "TEXT"]
        self.primary_key = ["run_id"]

    def start_run(
        self,
        seed_url,
        max_pages,
    ):
        """Create a new run record when crawler starts"""
        self.cursor.execute(
            """INSERT INTO runs (seed_url, start_time, max_pages)
                              VALUES (?, datetime('now'), ?);""",
            (seed_url, max_pages),
        )
        self.connection.commit()
        return self.cursor.lastrowid

    def complete_run(self, run_id):
        """Mark a run as completed and set end time"""
        self.cursor.execute(
            """UPDATE runs SET
                            end_time = datetime('now')
                            WHERE run_id = ?;""",
            (run_id),
        )
        self.connection.commit()


class UrlTable(BaseTable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = [
            "id",
            "seed_url",
            "url",
            "content",
            "req_status",
            "crawl_status",
            "run_id",
            "links",
            "created_at",
        ]
        self.types = [
            "INTEGER",
            "TEXT",
            "TEXT",
            "BLOB",
            "TEXT",
            "INTEGER",
            "INTEGER",
            "TEXT",
            "TEXT",
        ]
        self.primary_key = ["id"]

    def store_url(self, url_data: URLData, run_id: int, seed_url: str):
        """Store URL and its HTML content"""
        url = url_data.url
        content = url_data.content
        req_status = url_data.req_status
        crawl_status = url_data.crawl_status
        try:
            self.conn.execute(
                "INSERT INTO url_html (seed_url, url, content, req_status, crawl_status, run_id) VALUES (?, ?, ?, ?, ?, ?)",
                (seed_url, url, content, req_status, crawl_status, run_id),
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            # Update if URL already exists for this run
            self.conn.execute(
                "UPDATE url_html SET content = ?, req_status = ?, crawl_status = ? WHERE url = ? AND run_id = ?",
                (content, req_status, crawl_status, url, run_id),
            )
            self.conn.commit()
        return True


class LinksTable(BaseTable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = ["id", "seed_url", "source_url", "linked_url", "created_at"]
        self.types = ["INTEGER", "TEXT", "TEXT", "TEXT", "TEXT"]
        self.primary_key = ["id"]

    def store_links(self, seed_url: str, source_url: str, linked_urls: list[str]):
        """Store multiple links from a source URL"""
        try:
            # Create list of tuples for executemany
            links_data = [
                (seed_url, source_url, linked_url) for linked_url in linked_urls
            ]

            self.conn.executemany(
                """INSERT INTO url_html (seed_url, source_url, linked_url)
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
        self.columns = [
            "id",
            "source_url",
            "index_url",
            "loc",
            "priority",
            "frequency",
            "modified",
            "status",
            "created_at",
        ]
        self.types = [
            "INTEGER",
            "TEXT",
            "TEXT",
            "TEXT",
            "REAL",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
        ]
        self.primary_key = ["id"]

    def store_sitemap_data(self, sitemap_details: dict):
        """Store sitemap metadata"""
        try:
            self.conn.execute(
                """
                INSERT INTO sitemap_data
                (source_url, index_url, loc, priority, frequency, modified, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sitemap_details["source_url"],
                    sitemap_details["index"],
                    sitemap_details["loc"],
                    sitemap_details["priority"],
                    sitemap_details["frequency"],
                    sitemap_details["modified"],
                    sitemap_details["status"],
                ),
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            # Update if entry already exists
            self.conn.execute(
                """
                UPDATE sitemap_data
                SET loc = ?, priority = ?, frequency = ?, modified = ?, status = ?
                WHERE source_url = ? AND index_url = ?
                """,
                (
                    sitemap_details["loc"],
                    sitemap_details["priority"],
                    sitemap_details["frequency"],
                    sitemap_details["modified"],
                    sitemap_details["status"],
                    sitemap_details["source_url"],
                    sitemap_details["index"],
                ),
            )
            self.conn.commit()
