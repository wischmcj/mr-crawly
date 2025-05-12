from __future__ import annotations

import asyncio
import atexit
from asyncio import Queue
from parser import Parser

import redis
from config.configuration import get_logger
# from manager import Manager
from downloader import SiteDownloader
from manager import Manager

logger = get_logger("crawler")

rdb = redis.Redis(host="localhost", port=7777)

manager = Manager(host="localhost", port=7777, db_file="sqlite.db", rdb_file="data.rdb")


async def process_url_while_true(url: str, retries: int, max_pages: int):
    parse_queue = Queue(20)
    manager.visit_tracker.add_page_to_visit(url)
    print(f"Added {url} to visit tracker")
    download_producer = asyncio.create_task(
        download_url_while_true(parse_queue, url, retries, max_pages)
    )
    parsed_results = parse_while_true(parse_queue)
    all_links = await asyncio.gather(download_producer, parsed_results)
    logger.info(f"Completed processing {url}")
    return all_links


async def download_url_while_true(
    parse_queue: Queue, url: str, retries: int, max_pages
):
    """
    Periodically check the queue for new items requested for download.
    If there are new items, download them and add them to the queue.
    If there are no new items, sleep for a short period of time and check again.
    """
    downloader = SiteDownloader(manager)
    check_every = 0.2
    empty_count = 0
    max_empty_count = 25
    page_count = 0
    while True and empty_count <= max_empty_count and page_count < max_pages:
        url = manager.visit_tracker.get_page_to_visit()
        if url is not None:
            if not manager.visit_tracker.is_page_visited(url):
                try_num = 0
                print(f"Content received for {url} ")
                while try_num <= retries:
                    try:
                        manager.visit_tracker.add_page_visited(url)
                        content, status = downloader.get_page_elements(url)
                        await parse_queue.put((url, content))
                        try_num = retries + 1
                        page_count += 1
                    except Exception as e:
                        logger.error(f"Error downloading page {url}: {e}")
                        if "429" in str(e):  # too many requests
                            await asyncio.sleep(10)
                        else:
                            try_num += 1
                        # on_download_failure(url)
        else:
            empty_count += 1
        await asyncio.sleep(check_every)
    if page_count >= max_pages:
        logger.info(f"Maximum number of pages reached: {max_pages}")
    if empty_count >= max_empty_count:
        logger.info(f"Queue empty for {max_empty_count} consecutive checks")
    logger.info(f"Completed processing {url}")
    return


async def parse_while_true(parse_queue: Queue):
    """
    Parse the content of a page, extract urls.
    Pass extracted links back into queue for download
    """
    links = []
    parser = Parser(manager)
    while True:
        url, content = await parse_queue.get()
        print(f"Content received for {url} ")
        link_list = parser.get_links_from_content(url, content)  # .get_links(content)
        for link in link_list:
            links.append(link)
            print(f"{url=}, {link=}")

        parse_queue.task_done()


def crawl(seed_url: str, max_pages: int, retries: int):
    # main()
    atexit.register(manager.shutdown)
    manager.seed_url = seed_url
    manager.max_pages = max_pages
    manager.retries = retries
    manager.db_manager.start_run(manager.run_id, seed_url, max_pages)
    print(f"Starting crawl for {seed_url}")
    links = asyncio.run(
        process_url_while_true(url=seed_url, retries=retries, max_pages=max_pages)
    )

    print(links)


if __name__ == "__main__":
    crawl(seed_url="https://overstory.com", max_pages=10, retries=3)
