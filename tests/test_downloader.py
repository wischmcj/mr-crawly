from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from simple_crawler.downloader import SiteDownloader


@pytest.fixture
def mock_manager():
    manager = Mock()
    manager.cache = Mock()
    manager.visit_tracker = Mock()
    manager.db_manager = Mock()
    return manager


@pytest.fixture
def downloader(mock_manager):
    return SiteDownloader(manager=mock_manager)


def test_on_success(downloader):
    """Test successful download handling"""
    url = "https://example.com"
    content = "<html>test</html>"
    status_code = 200

    downloader.on_success(url, content, status_code)

    downloader.manager.db_manager.update_content.assert_called_once_with(
        url, content, status_code
    )
    downloader.manager.crawl_tracker.update_status.assert_called_once_with(
        url, "downloaded"
    )


def test_on_failure(downloader):
    """Test failed download handling"""
    url = "https://example.com"
    crawl_status = "error"
    content = "<html>error</html>"
    status_code = 404

    downloader.manager.crawl_tracker.update_status.return_value = {"some": "data"}

    downloader.on_failure(url, crawl_status, content, status_code)

    downloader.manager.db_manager.update_content.assert_called_once_with(
        url, content, status_code
    )
    downloader.manager.crawl_tracker.update_status.assert_called_once_with(
        url, crawl_status
    )
    downloader.manager.db_manager.store_url.assert_called_once_with(
        {"some": "data"}, downloader.manager.run_id, downloader.manager.seed_url
    )


@patch("simple_crawler.downloader.requests")
def test_get_page_elements_cached(mock_requests, downloader):
    """Test getting page elements when response is cached"""
    url = "https://example.com"
    cached_content = "<html>cached</html>"
    cached_status = 200

    downloader.manager.cache.get_cached_response.return_value = (
        cached_content,
        cached_status,
    )

    content, status = downloader.get_page_elements(url)

    assert content == cached_content
    assert status == cached_status
    mock_requests.get.assert_not_called()


@patch("simple_crawler.downloader.requests")
def test_get_page_elements_disallowed(mock_requests, downloader):
    """Test getting page elements when URL is disallowed"""
    url = "https://example.com/private"
    downloader.manager.cache.get_cached_response.return_value = (None, None)

    with patch.object(downloader, "can_fetch", return_value=False):
        content, status = downloader.get_page_elements(url)

        assert content == ""
        assert status == 403
        mock_requests.get.assert_not_called()
        downloader.manager.visit_tracker.add_page_visited.assert_called_once_with(url)
        # I had trouble mocking on_failure, so I just confirmed the contained methods were called
        downloader.manager.db_manager.update_content.assert_called_once_with(
            url, content, status
        )
        downloader.manager.crawl_tracker.update_status.assert_called_once_with(
            url, "disallowed"
        )
        downloader.manager.db_manager.store_url.assert_called()


@patch("simple_crawler.downloader.requests")
def test_get_page_elements_success(mock_requests, downloader):
    """Test getting page elements with successful request"""
    url = "https://example.com"
    response_content = "<html>test</html>"
    status_code = 200

    mock_response = Mock()
    mock_response.text = response_content
    mock_response.status_code = status_code
    mock_requests.get.return_value = mock_response

    downloader.manager.cache.get_cached_response.return_value = (None, None)
    with patch.object(downloader, "can_fetch", return_value=True):
        content, status = downloader.get_page_elements(url)

        assert content == response_content
        assert status == status_code
        # I had trouble mocking on_success, so I just confirmed the contained methods were called
        mock_requests.get.assert_called_once_with(url, timeout=10)
        downloader.manager.db_manager.update_content.assert_called_once_with(
            url, response_content, status_code
        )
        downloader.manager.crawl_tracker.update_status.assert_called_once_with(
            url, "downloaded"
        )
