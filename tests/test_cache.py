from __future__ import annotations

import pytest
import redis

from simple_crawler.cache import CrawlStatus, CrawlTracker, URLCache, URLData


@pytest.fixture
def redis_conn():
    return redis.Redis(host="localhost", port=7777, decode_responses=False)


@pytest.fixture
def url_cache(redis_conn):
    return URLCache(redis_conn)


@pytest.fixture
def crawl_tracker(redis_conn):
    return CrawlTracker(redis_conn)


@pytest.fixture
def sample_url():
    return "https://example.com"


@pytest.fixture
def sample_html_content():
    return "<html>Test</html>"


@pytest.fixture
def sample_url_data(sample_url, sample_html_content):
    return URLData(
        url=sample_url,
        content=sample_html_content,
        req_status=200,
        crawl_status=CrawlStatus.FRONTIER.value,
        run_id="test_run",
    )


@pytest.fixture
def sample_failed_url_data(sample_url):
    return URLData(
        url=sample_url,
        content=sample_html_content,
        req_status=404,
        crawl_status=CrawlStatus.ERROR.value,
        run_id="test_run",
    )


class TestURLData:
    def test_url_data_creation(self, sample_url_data):
        assert sample_url_data.url == "https://example.com"
        assert sample_url_data.req_status == 200
        assert sample_url_data.crawl_status == CrawlStatus.FRONTIER.value
        assert isinstance(sample_url_data.content, str)


class TestURLCache:
    def test_initialization(self, url_cache):
        assert isinstance(url_cache.visited_urls, set)
        assert isinstance(url_cache.to_visit, set)
        assert isinstance(url_cache.queues, list)

    @pytest.fixture(autouse=True)
    def teardown(self, redis_conn):
        yield
        redis_conn.flushall()

    def test_update_content_new_url(self, url_cache, sample_html_content):
        url = "https://example.com"
        content = sample_html_content
        status = "200"

        # Should work with string content
        url_cache.update_content(url, content, status)
        assert url_cache.rdb.hget(url, "content").decode() == content
        assert url_cache.rdb.hget(url, "req_status").decode() == status

    def test_update_content_non_string_content(self, url_cache):
        url = "https://example.com"
        content = {"html": "test content"}
        status = "200"

        with pytest.raises(ValueError):
            url_cache.update_content(url, content, status)

    def test_update_content_existing_url(self, url_cache, sample_html_content):
        # Add a url to the cache
        url_cache.rdb.hset("https://example.com", "", "404")

        url = "https://example.com"
        content = sample_html_content
        status = "200"

        url_cache.update_content(url, content, status)
        assert url_cache.rdb.hget(url, "content").decode() == content
        assert url_cache.rdb.hget(url, "req_status").decode() == status

    def test_get_cached_response(self, url_cache, sample_html_content):
        url = "https://example.com"
        content = sample_html_content
        status = 200

        url_cache.update_content(url, content, status)

        # Verify content was stored
        direct_stored_data = url_cache.rdb.hgetall(url)
        direct_stored_data = {
            k.decode(): v.decode() for k, v in direct_stored_data.items()
        }
        returned_stored_data = url_cache.get_cached_response(url)
        assert direct_stored_data["content"] == returned_stored_data[0]
        assert direct_stored_data["req_status"] == returned_stored_data[1]
