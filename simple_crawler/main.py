from __future__ import annotations

import asyncio
import atexit
import time
from asyncio import Queue
from parser import Parser

import redis
from config.configuration import (RDB_FILE, REDIS_HOST, REDIS_PORT,
                                  SQLITE_DB_FILE, get_logger)
# from manager import Manager
from downloader import SiteDownloader
from manager import Manager
from mapper import SiteMapper

logger = get_logger("crawler")

rdb = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

manager = Manager(
    host=REDIS_HOST, port=REDIS_PORT, db_file=SQLITE_DB_FILE, rdb_file=RDB_FILE
)


async def prime_queue(seed_url: str):
    # Check to see if we can get a sitemap
    mapper = SiteMapper(manager=manager, seed_url=seed_url)
    try:
        sitemap_url, sitemap_indexes, sitemap_details = mapper.get_sitemap()
    except Exception as e:
        logger.error(f"Error getting sitemap for {seed_url}: {e}")
        manager.crawl_tracker.add_page_to_visit(seed_url)


async def process_url_while_true(
    url: str, retries: int, write_to_db: bool = True, check_every: float = 0.5
):
    parse_queue = Queue(20)
    await prime_queue(url)
    logger.info(f"Primed queue for seed url {url}")
    download_producer = asyncio.create_task(
        download_url_while_true(parse_queue, retries, write_to_db, check_every)
    )
    parsed_results = parse_while_true(parse_queue, write_to_db, check_every)
    all_links = await asyncio.gather(download_producer, parsed_results)
    logger.info(f"Completed processing {url}")
    return all_links


async def download_url_while_true(
    parse_queue: Queue, retries: int, write_to_db: bool = True, check_every: float = 0.5
):
    """
    Periodically check the queue for new items requested for download.
    If there are new items, download them and add them to the queue.
    """
    downloader = SiteDownloader(manager, write_to_db)
    empty_count = 0
    max_empty_count = 25
    running = True
    # Continue querying cache for new items
    # Stop when 25 cycle have passed without finding any new items
    while running and empty_count <= max_empty_count:
        try:
            # Check redis list for new pages needing to be visited
            url = manager.crawl_tracker.get_page_to_visit()
            if url == 0:
                logger.info("No more pages to visit, closing queue")
                running = False
            for _ in range(retries):
                if url is not None:
                    logger.debug(f"Download request received for {url} ...")
                    content = None
                    try:
                        content, status = downloader.get_page_elements(url)
                    except Exception as e:
                        logger.error(f"Error downloading page {url}: {e}")
                        if "429" in str(e):
                            logger.info(
                                f"429 error, sleeping then increasing check_every to {check_every*1.5}"
                            )
                            check_every = check_every * 1.5
                            await asyncio.sleep(10)
                    manager.crawl_tracker.add_page_visited(url)
                    if content is not None:
                        await asyncio.wait_for(
                            parse_queue.put((url, content)), timeout=1
                        )
                    logger.debug("try loop exit")
                else:
                    empty_count += 1
            await asyncio.sleep(check_every)
        except asyncio.TimeoutError:
            logger.info("Timeout error")
            break
    logger.info(f"Completed processing {url}, exiting...")
    await asyncio.sleep(5)
    return


async def parse_while_true(
    parse_queue: Queue, write_to_db: bool = True, check_every: float = 0.5
):
    """
    Parse the content of a page, extract urls.
    Pass extracted links back into queue for download
    """
    links = []
    parser = Parser(manager, write_to_db)
    empty_count = 0
    max_empty_count = 25
    # Continue parsing until 25 cycle have passed without finding any new items
    # More likely, the max page limit will be reached first, causing the downloader
    # to exit
    while True and empty_count <= max_empty_count:
        if not parse_queue.empty():
            empty_count = 0
            url, content = await asyncio.wait_for(parse_queue.get(), timeout=1)
            logger.info(f"Content received for {url}, parsing...")
            link_list = parser.parse(url, content)
            for link in link_list:
                links.append(link)
            if len(links) == 0:
                logger.warning(f"No links found for {url}")
            parse_queue.task_done()
        else:
            empty_count += 1
        await asyncio.sleep(check_every)

    if empty_count >= max_empty_count:
        logger.info(f"Queue empty for {max_empty_count} consecutive checks")
    logger.info("Completed parsing")
    return links


def crawl(
    seed_url: str,
    max_pages: int = 100,
    retries: int = 3,
    write_to_db: bool = True,
    check_every: float = 0.5,
):
    # main()
    atexit.register(manager.shutdown)
    manager.set_seed_url(seed_url)
    manager.set_max_pages(max_pages)
    manager.retries = retries
    manager.db_manager.start_run(manager.run_id, seed_url, max_pages)
    logger.info(f"Starting crawl for {seed_url}")
    links = asyncio.run(
        process_url_while_true(
            url=seed_url,
            retries=retries,
            write_to_db=write_to_db,
            check_every=check_every,
        )
    )
    time.sleep(3)
    return links


if __name__ == "__main__":
    crawl(
        seed_url="https://www.overstory.com",
        max_pages=10,
        retries=1,
        write_to_db=True,
        check_every=0.2,
    )
