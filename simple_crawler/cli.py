from __future__ import annotations

import argparse

from config.logging_config import get_logger
from main_pc import crawl


def main():
    logger = get_logger("crawler")
    logger.info("Starting crawler")
    parser = argparse.ArgumentParser(description="Basic Web Crawler")
    parser.add_argument("url", help="Starting URL to crawl")
    parser.add_argument(
        "--max-pages", type=int, default=10, help="Maximum number of pages to crawl"
    )
    parser.add_argument(
        "--retries", type=int, default=3, help="Delay between requests in seconds"
    )

    args = parser.parse_args()

    crawl(args.url, args.max_pages, args.retries)
