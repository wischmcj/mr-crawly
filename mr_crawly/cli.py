from __future__ import annotations

import argparse

from config.configuration import CHECK_EVERY, MAX_PAGES, RETRIES, get_logger
from main import crawl

logger = get_logger("main")
logger.info("Starting crawler")
parser = argparse.ArgumentParser(description="Basic Web Crawler")
parser.add_argument("url", help="Starting URL to crawl")
parser.add_argument(
    "--max-pages", type=int, default=MAX_PAGES, help="Maximum number of pages to crawl"
)
parser.add_argument(
    "--retries", type=int, default=RETRIES, help="Delay between requests in seconds"
)
parser.add_argument(
    "--check_every",
    type=float,
    default=CHECK_EVERY,
    help="Delay between checks in seconds",
)
args = parser.parse_args()
links = crawl(args.url, args.max_pages, args.retries, args.check_every)
for link in links:
    logger.info(link)
logger.info(f"Crawled {len(links)} pages")
