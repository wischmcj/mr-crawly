from __future__ import annotations

import os
from unittest.mock import Mock, patch

import pytest

from simple_crawler.manager import Manager


@pytest.fixture
def mock_redis():
    with patch("redis.Redis") as mock:
        yield mock


@pytest.fixture
def manager():
    return Manager(
        seed_url="https://example.com",
        max_pages=10,
        host="localhost",
        port=7777,
        retries=3,
        debug=False,
        db_file="test.db",
    )


# @pytest.mark.skip(reason="Requires running Redis instance")
def test_save_cache_integration(manager):
    """Integration test for save_cache with real Redis"""
    manager.save_cache()
    assert os.path.exists(manager.rdb_path)


def test_manager_initialization(manager):
    """Test manager initializes with correct attributes"""
    assert manager.seed_url == "https://example.com"
    assert manager.max_pages == 10
    assert manager.retries == 3
    assert manager.is_async is True
    assert isinstance(manager.visited_urls, set)
    assert isinstance(manager.to_visit, set)
    assert isinstance(manager.listeners, list)


def test_get_run_data(manager):
    """Test get_run_data returns correct data dictionary"""
    data = manager.get_run_data()
    assert data["seed_url"] == "https://example.com"
    assert data["max_pages"] == 10
    assert data["retries"] == 3
    assert data["is_async"] is True
    assert "run_id" in data


def test_needs_visitation(manager):
    """Test needs_visitation logic"""
    # Mock cache.is_page_visited to return False
    manager.vtracker.is_page_visited = Mock(return_value=False)
    assert manager.needs_visitation("https://example.com/new") is True

    # Mock cache.is_page_visited to return True
    manager.vtracker.is_page_visited = Mock(return_value=True)
    assert manager.needs_visitation("https://example.com/visited") is False


def test_below_page_limit(manager):
    """Test below_page_limit logic"""
    # Mock cache to return list smaller than max_pages
    manager.cache.get_pages_visited = Mock(return_value=["url1", "url2"])
    assert manager.below_page_limit() is True

    # Mock cache to return list equal to max_pages
    manager.shutdown = Mock(return_value=False)
    manager.cache.get_pages_visited = Mock(return_value=["url" for _ in range(10)])
    assert manager.below_page_limit() is False


def test_init_dirs(manager):
    """Test directory initialization"""
    assert os.path.exists(manager.data_dir)
    assert manager.data_dir.endswith(manager.run_id)
    assert manager.rdb_path.endswith("data.rdb")
    assert manager.sqlite_path.endswith(manager.db_file)


def test_flush_db(manager):
    """Test database flush"""
    manager.rdb.flushdb = Mock(True)
    manager._flush_db()
    manager.rdb.flushdb.assert_called_once()
