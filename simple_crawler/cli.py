from __future__ import annotations

import argparse
import asyncio

from config.logging_config import get_logger

from simple_crawler.main import crawl


def main():
    logger = get_logger("crawler")
    logger.info("Starting crawler")
    parser = argparse.ArgumentParser(description="Basic Web Crawler")
    parser.add_argument("url", help="Starting URL to crawl")
    parser.add_argument(
        "--max-pages", type=int, default=100, help="Maximum number of pages to crawl"
    )
    parser.add_argument(
        "--retries", type=int, default=3, help="Delay between requests in seconds"
    )
    parser.add_argument(
        "--write_to_db",
        type=bool,
        default=True,
        help="Determimes if results are saved to a sqlite db",
    )
    parser.add_argument(
        "--check_every", type=float, default=0.5, help="Delay between checks in seconds"
    )

    args = parser.parse_args()

    links = asyncio.run(
        crawl(
            args.url, args.max_pages, args.retries, args.write_to_db, args.check_every
        )
    )
    for link in links:
        print(link)
    print(f"Crawled {len(links)} pages")
