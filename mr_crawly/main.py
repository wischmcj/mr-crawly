from __future__ import annotations

import argparse
from datetime import datetime
from logging import Manager

from config.configuration import get_logger


def main():
    logger = get_logger("crawler")
    logger.info("Starting crawler")
    parser = argparse.ArgumentParser(description="Basic Web Crawler")
    parser.add_argument("url", help="Starting URL to crawl")

    # Included for testing mostly, but could be used to just pull web data for single page
    parser.add_argument("--parse", action=argparse.BooleanOptionalAction, default=True)

    parser.add_argument(
        "--max-pages", type=int, default=10, help="Maximum number of pages to crawl"
    )
    parser.add_argument(
        "--delay", type=float, default=1.0, help="Delay between requests in seconds"
    )

    args = parser.parse_args()

    # Initialize URL/HTML storage
    formatted_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    print("Formatted datetime:", formatted_datetime)

    manager = Manager(
        seed_url=args.url, max_pages=args.max_pages, delay=args.delay, parse=args.parse
    )
    manager.get_to_work()


if __name__ == "__main__":
    main()
