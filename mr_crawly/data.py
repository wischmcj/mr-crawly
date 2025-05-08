from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass

from config.configuration import get_logger


@dataclass
class Run:
    run_id: int
    seed_url: str
    start_time: str
    max_pages: int
    delay: float
    pages_crawled: int | None = None
    end_time: str | None = None
    status: str = "running"
    error: str | None = None


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
                                end_time TEXT NULL,
                                pages_crawled INTEGER DEFAULT 0,
                                max_pages INTEGER,
                                delay REAL,
                                status TEXT DEFAULT 'running',
                                error TEXT NULL
                            );"""
        )
        self.connection.commit()

    def start_run(self, seed_url, max_pages, delay):
        """Create a new run record when crawler starts"""
        self.cursor.execute(
            """INSERT INTO runs (seed_url, start_time, max_pages, delay)
                              VALUES (?, datetime('now'), ?, ?);""",
            (seed_url, max_pages, delay),
        )
        self.connection.commit()
        return self.cursor.lastrowid

    def update_run(self, run_id, pages_crawled=None, status=None, error=None):
        """Update run statistics during crawling"""
        updates = []
        params = []
        if pages_crawled is not None:
            updates.append("pages_crawled = ?")
            params.append(pages_crawled)
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if error is not None:
            updates.append("error = ?")
            params.append(error)

        if updates:
            query = f"""UPDATE runs SET {', '.join(updates)}
                       WHERE run_id = ?;"""
            params.append(run_id)
            self.cursor.execute(query, params)
            self.connection.commit()

    def complete_run(self, run_id, status="completed"):
        """Mark a run as completed and set end time"""
        self.cursor.execute(
            """UPDATE runs SET
                                status = ?,
                                end_time = datetime('now')
                              WHERE run_id = ?;""",
            (status, run_id),
        )
        self.connection.commit()

    def get_run(self, run_id):
        """Get details for a specific run"""
        self.cursor.execute("SELECT * FROM runs WHERE run_id = ?;", (run_id,))
        return self.cursor.fetchone()


class UrlHTML:
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
                html BLOB,
                run_id INTEGER,
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

    def get_urls_for_run(self, run_id: int) -> list[str]:
        """Get all URLs stored for a specific run"""
        cursor = self.conn.execute(
            "SELECT url FROM url_html WHERE run_id = ?", (run_id,)
        )
        return [row[0] for row in cursor.fetchall()]

    def __del__(self):
        """Close database connection on cleanup"""
        self.conn.close()


def initialize_db(db_path: str):
    """Initialize database with all tables and return connected objects"""
    # Ensure data directory exists
    data_dir = os.path.dirname(db_path)
    os.makedirs(data_dir, exist_ok=True)

    # Get logger
    logger = get_logger(__name__)

    # Initialize tables
    run_table = RunTable(db_path, logger)
    url_store = UrlHTML(db_path)

    return run_table, url_store
