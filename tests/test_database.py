from __future__ import annotations

import sqlite3
import unittest
from dataclasses import dataclass

from simple_crawler.data import DatabaseManager


@dataclass
class TestURLData:
    url: str
    content: str
    req_status: str
    crawl_status: int


class TestDatabase(unittest.TestCase):
    def setUp(self):
        # Create in-memory database for testing
        self.conn = sqlite3.connect(":memory:")
        self.db = DatabaseManager(self.conn)
        self.db.create_tables()

    def tearDown(self):
        self.conn.close()

    def test_run_table(self):
        # Test starting a new run
        run_id = self.db.start_run("https://test.com", 100)
        self.assertIsInstance(run_id, int)

        # Test completing a run
        self.db.complete_run(run_id)

        # Verify run was stored
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        run = cursor.fetchone()

        self.assertEqual(run[1], "https://test.com")  # seed_url
        self.assertEqual(run[3], 100)  # max_pages
        self.assertIsNotNone(run[4])  # end_time

    def test_url_table(self):
        run_id = self.db.start_run("https://test.com", 100)

        # Test storing URL data
        url_data = TestURLData(
            url="https://test.com/page1",
            content="<html>Test</html>",
            req_status="200",
            crawl_status=1,
        )

        self.db.store_url(url_data, run_id, "https://test.com")

        # Verify URL was stored
        urls = self.db.get_urls_for_run(run_id)
        self.assertEqual(len(urls), 1)
        self.assertEqual(urls[0].url, "https://test.com/page1")

    def test_links_table(self):
        run_id = self.db.start_run("https://test.com", 100)

        # Test storing links
        self.db.store_link("https://test.com", "https://test.com/page1", run_id)

        # Verify links were stored
        links = self.db.get_links_for_run(run_id)
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0], ("https://test.com", "https://test.com/page1"))

    def test_sitemap_table(self):
        run_id = self.db.start_run("https://test.com", 100)

        # Test storing sitemap
        sitemap_data = {
            "source_url": "https://test.com",
            "index": "https://test.com/sitemap.xml",
            "loc": "https://test.com/page1",
            "priority": 0.8,
            "frequency": "daily",
            "modified": "2023-01-01",
            "status": "success",
        }

        self.db.store_sitemap("https://test.com", sitemap_data, run_id)

        # Verify sitemap was stored
        sitemaps = self.db.get_sitemaps_for_run(run_id)
        self.assertEqual(len(sitemaps), 1)
        self.assertEqual(sitemaps[0]["loc"], "https://test.com/page1")


if __name__ == "__main__":
    unittest.main()
