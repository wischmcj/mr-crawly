from __future__ import annotations

import unittest
from unittest.mock import Mock, patch
from urllib.parse import urljoin

from simple_crawler.parser import Parser


class TestParser(unittest.TestCase):
    def setUp(self):
        self.mock_manager = Mock()
        self.mock_manager.cache = Mock()
        self.mock_manager.crawl_tracker = Mock()
        self.mock_manager.db_manager = Mock()
        self.parser = Parser(manager=self.mock_manager)

    def test_get_links_from_content(self):
        """Test extracting links from HTML content"""
        test_url = "https://example.com"
        test_content = """
        <html>
            <body>
                <a href="/page1">Page 1</a>
                <a href="https://example.com/page2">Page 2</a>
                <a href="https://other-domain.com/page3">Page 3</a>
            </body>
        </html>
        """

        links = self.parser.get_links_from_content(test_url, test_content)

        expected_links = {urljoin(test_url, "/page1"), "https://example.com/page2"}

        self.assertEqual(links, expected_links)
        self.mock_manager.crawl_tracker.add_page_to_visit.assert_called()

    def test_on_success(self):
        """Test successful parsing callback"""
        test_url = "https://example.com"
        self.parser.on_success(test_url, ["https://example.com"])
        self.mock_manager.crawl_tracker.store_linked_urls.assert_called_with(
            test_url, ["https://example.com"]
        )
        self.mock_manager.crawl_tracker.update_status.assert_called_with(
            test_url, "parsed"
        )

    def test_on_failure(self):
        """Test failure parsing callback"""
        test_url = "https://example.com"
        self.parser.on_failure(test_url)
        self.mock_manager.crawl_tracker.update_status.assert_called()

    def test_parse_success(self):
        """Test successful parsing of a page"""
        test_url = "https://example.com"
        test_content = "<html><a href='/test'>Test</a></html>"

        self.parser.url = test_url
        self.parser.parse(test_url, test_content)

        self.mock_manager.crawl_tracker.update_status.assert_called_with(
            test_url, "parsed"
        )

    def test_on_failure_called_after_exception(self):
        """Test failure parsing callback"""
        with patch(
            "simple_crawler.parser.Parser.get_links_from_content",
            side_effect=Exception("mocked error"),
        ):
            test_url = "https://example.com"
            test_content = "<html><a href='/test'>Test</a></html>"
            self.parser.parse(url=test_url, content=test_content)
            self.mock_manager.crawl_tracker.update_status.assert_called_with(
                test_url, "error"
            )

    def test_parse_failure(self):
        """Test parsing with an error"""
        test_url = "https://example.com"
        test_content = None  # Invalid content to trigger exception

        self.parser.url = test_url
        self.parser.parse(test_url, test_content)

        self.mock_manager.crawl_tracker.update_status.assert_called_with(
            test_url, "error"
        )

    def test_invalid_href(self):
        """Test handling of invalid href attributes"""
        test_url = "https://example.com"
        test_content = "<html><a href='javascript:void(0)'>Invalid</a></html>"

        links = self.parser.get_links_from_content(test_url, test_content)
        self.assertEqual(links, set())
