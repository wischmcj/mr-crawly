from __future__ import annotations

from datetime import timedelta

import redis


class PageCache:
    def __init__(self, host="localhost", port=7777, expiry_hours=24):
        """Initialize Redis cache for storing webpage HTML"""
        self.redis = redis.Redis(host=host, port=port, decode_responses=True)
        self.expiry = timedelta(hours=expiry_hours)

    def callback(self, request):
        return True

    def store_page(self, url: str, html: str) -> None:
        """Store webpage HTML in cache with expiration"""
        # if not can_cache(conn, request):
        #     return False
        self.redis.setex(name=url, time=self.expiry, value=html)

    def get_page(self, url: str) -> str | None:
        """Retrieve cached webpage HTML if available"""
        return self.redis.get(url)

    def delete_page(self, url: str) -> None:
        """Remove a page from cache"""
        self.redis.delete(url)

    def clear_cache(self) -> None:
        """Clear all cached pages"""
        self.redis.flushall()
