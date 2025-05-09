from __future__ import annotations

import argparse
import atexit
from datetime import datetime
from logging import Manager

from config.configuration import get_logger


def main():
    logger = get_logger("crawler")
    logger.info("Starting crawler")
    parser = argparse.ArgumentParser(description="Basic Web Crawler")
    parser.add_argument("url", help="Starting URL to crawl")
    parser.add_argument(
        "--max-pages", type=int, default=10, help="Maximum number of pages to crawl"
    )
    parser.add_argument(
        "--num_workers", type=int, default=1, help="Delay between requests in seconds"
    )
    parser.add_argument(
        "--retries", type=int, default=3, help="Delay between requests in seconds"
    )
    parser.add_argument(
        "--debug",
        type=int,
        default=False,
        help="If false, all operations will run synchronously",
    )

    args = parser.parse_args()

    # Initialize URL/HTML storage
    formatted_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    print("Formatted datetime:", formatted_datetime)

    manager = Manager(
        seed_url=args.url,
        max_pages=args.max_pages,
        num_workers=args.num_workers,
        retries=args.retries,
        debug=args.debug,
    )
    atexit.register(manager.shutdown)
    manager.get_to_work()


if __name__ == "__main__":
    main()
