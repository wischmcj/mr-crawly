from __future__ import annotations

from urllib.parse import urlparse


def parse_url(url: str):
    """Parse a url and return the page elements"""
    parsed_url = urlparse(url)
    return parsed_url.scheme, parsed_url.netloc, parsed_url.path
