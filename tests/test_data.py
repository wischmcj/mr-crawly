from __future__ import annotations

import sqlite3

import pytest
import redis

from simple_crawler.data import (BaseTable, DatabaseManager, Run, RunTable,
                                 SitemapTable, UrlBulkWriter, UrlTable)


@pytest.fixture
def db_file():
    return "data/test.db"


@pytest.fixture
def conn(db_file):
    connection = sqlite3.connect(db_file)
    yield connection
    connection.close()


@pytest.fixture
def db_manager(db_file):
    manager = DatabaseManager(db_file=db_file)
    return manager


@pytest.fixture
def base_table(conn):
    table = BaseTable(conn)
    return table


@pytest.fixture
def run():
    return Run(
        run_id="1",
        seed_url="http://example.com",
        start_time="2023-01-01 00:00:00",
        max_pages=100,
        end_time=None,
    )


@pytest.fixture
def run_table(conn):
    table = RunTable(conn)
    table.create_table()
    return table


@pytest.fixture
def url_table(conn):
    table = UrlTable(conn)
    table.create_table()
    return table


@pytest.fixture
def sitemap_table(conn):
    table = SitemapTable(conn)
    table.create_table()
    return table


@pytest.fixture
def url_data():
    return {
        "url": "http://example.com/page1",
        "content": "test content",
        "req_status": "200",
        "crawl_status": "frontier",
        "parent_url": "http://example.com",
        "seed_url": "http://example.com",
        "run_id": "1",
    }


@pytest.fixture
def incomplete_url_data():
    return {
        "seed_url": "https://www.overstory.com",
        "run_id": "2025_05_12_15_47_34",
        "crawl_status": "downloaded",
        "url": "https://www.overstory.com/blog",
        "req_status": 200,
    }


@pytest.fixture
def sitemap_data():
    return {
        "run_id": "1",
        "url": "https://www.google.com/gmail/sitemap.xml",
        "index_url": "https://www.google.com/gmail/sitemap.xml",
        "seed_url": "https://www.google.com/",
        "loc": "https://www.google.com/intl/am/gmail/about/",
        "priority": None,
        "frequency": None,
        "modified": None,
        "status": "Success",
    }


@pytest.fixture
def redis_conn():
    return redis.Redis(host="localhost", port=7777, decode_responses=False)


class TestBaseTable:
    def setup(self):
        self.base_table.cursor.execute("DROP TABLE IF EXISTS test").commit()

    def teardown(self):
        self.base_table.cursor.execute("DROP TABLE IF EXISTS test").commit()

    def test_build_create_string(self, base_table):
        base_table.table_name = "test"
        base_table.columns = ["id", "name"]
        base_table.types = ["INTEGER", "TEXT"]
        base_table.primary_key = "id"
        base_table.unique_keys = ["id"]
        create_string = base_table.build_create_string()
        assert "CREATE TABLE IF NOT EXISTS test" in create_string
        assert "id INTEGER PRIMARY KEY AUTOINCREMENT" in create_string
        assert "name TEXT" in create_string
        assert "created_at TIMESTAMP" in create_string

    def test_create_table(self, base_table):
        base_table.table_name = "test"
        base_table.columns = ["id", "name"]
        base_table.types = ["INTEGER", "TEXT"]
        base_table.primary_key = "id"
        base_table.unique_keys = ["id"]

        base_table.create_table()
        # Verify table exists
        base_table.cursor.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='test';
        """
        )
        assert base_table.cursor.fetchone() is not None


class TestUrlTable:
    def teardown(self):
        url_table.cursor.execute("DELETE FROM urls")

    def test_store_url(self, url_table, url_data):
        url_table.cursor.execute("DELETE FROM urls")

        url_id = url_table.store_urls([url_data])
        assert url_id is not None
        # Verify URL was stored
        url_table.cursor.execute("SELECT * FROM urls WHERE id = ?", (url_id,))
        result = url_table.cursor.fetchone()
        assert result is not None
        assert result[1] == url_data["seed_url"]

    def test_get_urls_for_seed_url(self, url_table, url_data):
        seed_url = "http://example.com"

        urls = url_table.get_urls_for_seed_url(seed_url)
        assert len(urls) == 1
        assert urls[0]["url"] == url_data["url"]

    def test_get_urls_for_run(self, url_table, url_data):
        run_id = "1"
        urls = url_table.get_urls_for_run(run_id)
        assert len(urls) == 1
        assert urls[0]["url"] == url_data["url"]


class TestRunTable:
    def teardown(self):
        run_table.cursor.execute("DELETE FROM runs")

    def test_start_run(self, run_table):
        run_table.cursor.execute("DELETE FROM runs")
        run_id = "1"
        seed_url = "http://example.com"
        max_pages = 10

        row_id = run_table.start_run(run_id, seed_url, max_pages)
        assert row_id is not None

        # Verify run was created
        run_table.cursor.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        result = run_table.cursor.fetchone()
        assert result is not None
        assert result[1] == run_id
        assert result[2] == seed_url
        assert result[4] == max_pages
        assert result[5] is None  # end_time should be null

    def test_complete_run(self, run_table):
        run_id = "1"
        success = run_table.complete_run(run_id)
        assert success is True

        # Verify run was completed
        run_table.cursor.execute(
            "SELECT end_time FROM runs WHERE run_id = ?", (run_id,)
        )
        result = run_table.cursor.fetchone()
        assert result is not None
        assert result[0] is not None


class TestSitemapTable:
    def teardown(self):
        sitemap_table.cursor.execute("DELETE FROM sitemaps")

    def test_store_sitemap(self, sitemap_table, sitemap_data):
        sitemap_table.cursor.execute("DELETE FROM sitemaps")
        sitemap_table.store_sitemap_data(
            sitemap_data, seed_url="http://example.com", run_id="1"
        )

        # Verify sitemap was stored
        sitemap_table.cursor.execute(
            "SELECT * FROM sitemaps WHERE url = ?", (sitemap_data["url"],)
        )
        result = sitemap_table.cursor.fetchone()
        assert result is not None
        assert result[3] == sitemap_data["url"]

    def test_get_sitemaps_for_seed_url(self, sitemap_table, sitemap_data):
        sitemap_table.cursor.execute("DELETE FROM sitemaps")
        sitemap_table.store_sitemap_data(
            sitemap_data, seed_url="http://example.com", run_id="1"
        )
        seed_url = sitemap_data["seed_url"]
        sitemaps = sitemap_table.get_sitemaps_for_seed_url(seed_url)
        assert len(sitemaps) == 1
        assert sitemaps[0]["url"] == sitemap_data["url"]


class MockPubSub:
    def __init__(self):
        self.messages = []

    def listen(self):
        try:
            return self.messages.pop(0)
        except IndexError:
            return []


class TestUrlBulkWriter:
    def test_store_url(self, db_file):
        pubsub = MockPubSub()
        writer = UrlBulkWriter(pubsub, db_file, batch_size=2)

        url_data = {"url": "http://example.com", "run_id": "1"}
        writer.store_url(url_data)

        # First URL should be buffered
        assert len(writer.urls_to_write) == 1
        assert writer.urls_to_write[0] == url_data

        # Second URL should trigger flush
        writer.store_url(url_data)
        assert len(writer.urls_to_write) == 2

        writer.store_url(url_data)
        assert len(writer.urls_to_write) == 0  # Should have flushed

    def test_flush(self, db_file):
        pubsub = MockPubSub()
        writer = UrlBulkWriter(pubsub, db_file)

        url_data = {"url": "http://example.com", "run_id": "1"}
        writer.store_url(url_data)
        writer.store_url(url_data)

        assert len(writer.urls_to_write) == 2
        writer.flush_urls()
        assert len(writer.urls_to_write) == 0
