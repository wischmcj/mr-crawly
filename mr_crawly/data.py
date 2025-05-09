from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from config.configuration import get_logger


@dataclass
class Run:
    run_id: int
    seed_url: str
    start_time: str
    max_pages: int
    end_time: str | None = None


class RunTable:
    def __init__(self, db_file, logger=None):
        self.connection = sqlite3.connect(db_file)
        self.cursor = self.connection.cursor()
        self.create_table()
        self.logger = logger or get_logger(__name__)

    def create_table(self):
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS runs (
                                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                seed_url TEXT NOT NULL,
                                start_time TEXT NOT NULL,
                                max_pages INTEGER,
                                end_time TEXT NULL
                            );"""
        )
        self.connection.commit()

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

    def complete_run(self, run_id, status="completed"):
        """Mark a run as completed and set end time"""
        self.cursor.execute(
            """UPDATE runs SET
                            end_time = datetime('now')
                            WHERE run_id = ?;""",
            (run_id),
        )
        self.connection.commit()


class UrlTable:
    def __init__(self, db_path: str):
        """Initialize URL/HTML storage"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.create_tables()

    def create_tables(self):
        """Create URL/HTML storage tables if they don't exist"""
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS url_html (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                content BLOB,
                req_status INTEGER,
                run_id INTEGER,
                links TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(url, run_id)
            )
        """
        )
        self.conn.commit()

    def store_url_content(self, url: str, html_content: bytes, run_id: int):
        """Store URL and its HTML content"""
        try:
            self.conn.execute(
                "INSERT INTO url_html (url, html, run_id) VALUES (?, ?, ?)",
                (url, html_content, run_id),
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            # Update if URL already exists for this run
            self.conn.execute(
                "UPDATE url_html SET html = ? WHERE url = ? AND run_id = ?",
                (html_content, url, run_id),
            )
            self.conn.commit()

    def get_url_content(self, url: str) -> bytes:
        """Retrieve HTML content for a URL"""
        cursor = self.conn.execute(
            "SELECT html FROM url_html WHERE url = ? ORDER BY created_at DESC LIMIT 1",
            (url,),
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def url_exists(self, url: str, run_id: int = None) -> bool:
        """Check if URL has already been stored"""
        if run_id:
            cursor = self.conn.execute(
                "SELECT 1 FROM url_html WHERE url = ? AND run_id = ?", (url, run_id)
            )
        else:
            cursor = self.conn.execute("SELECT 1 FROM url_html WHERE url = ?", (url,))
        return cursor.fetchone() is not None


class LinksTable:
    def __init__(self, db_path: str):
        """Initialize URL/HTML storage"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.create_tables()

    def create_tables(self):
        """Create URL/HTML storage tables if they don't exist"""
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS url_html (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seed_url TEXT NOT NULL,
                source_url TEXT NOT NULL,
                linked_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(seed_url, source_url, linked_url)
            )
        """
        )
        self.conn.commit()

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


class SitemapTable:
    def __init__(self, db_path: str):
        """Initialize URL/HTML storage"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.create_table()

    def create_table(self):
        """Close database connection on cleanup"""
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sitemap_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_url TEXT NOT NULL,
                index_url TEXT,
                loc TEXT,
                priority REAL,
                frequency TEXT,
                modified TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_url, index_url)
            )
        """
        )
        self.conn.commit()

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

    def get_sitemap_data(self, source_url: str, index_url: str = None) -> dict:
        """Retrieve sitemap data"""
        if index_url:
            cursor = self.conn.execute(
                "SELECT * FROM sitemap_data WHERE source_url = ? AND index_url = ?",
                (source_url, index_url),
            )
        else:
            cursor = self.conn.execute(
                "SELECT * FROM sitemap_data WHERE source_url = ?", (source_url,)
            )
        result = cursor.fetchone()
        if result:
            return {
                "source_url": result[1],
                "index": result[2],
                "loc": result[3],
                "priority": result[4],
                "frequency": result[5],
                "modified": result[6],
                "status": result[7],
            }
        return None
