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


async def get_page_to_visit() -> list[str]:
    """Get all frontier seeds for a URL"""
    url = rdb.lpop("to_visit")
    if url is not None:
        url = url.decode("utf-8")
    return url


def add_page_to_visit(url: str):
    """Add a frontier URL to the visit queue"""
    channel = "to_visit"
    rdb.lpush(channel, url)


def add_page_visited(url: str):
    """Add a visited seed for a URL"""
    rdb.sadd("visited", url)


def get_pages_visited() -> list[str]:
    """Get all frontier seeds for a URL"""
    return rdb.smembers("visited")


def is_page_visited(url: str) -> bool:
    """Check if a page has been visited"""
    resp = rdb.sismember("visited", url)
    is_member = bool(resp)
    return is_member


async def process_url(url: str, retries: int):
    parse_queue = Queue(20)
    add_page_to_visit(url)
    download_producer = asyncio.create_task(download_url(parse_queue, url, retries))
    parsed_results = parse_page(parse_queue)
    all_links = await asyncio.gather(download_producer, parsed_results)
    logger.info(f"Completed processing {url}")
    return all_links


async def download_url(parse_queue: Queue, url: str, retries: int):
    """
    Periodically check the queue for new items requested for download.
    If there are new items, download them and add them to the queue.
    If there are no new items, sleep for a short period of time and check again.
    """
    downloader = SiteDownloader(manager, host="localhost", port=7777)
    check_every = 0.2
    empty_count = 0
    max_empty_count = 25
    while empty_count <= max_empty_count:
        url = await get_page_to_visit()
        if url is not None:
            if not is_page_visited(url):
                try_num = 0
                empty_count = 0
                print(f"Content received for {url} ")
                while try_num <= retries:
                    try:
                        add_page_visited(url)
                        content, status = downloader.get_page_elements(url)
                        await parse_queue.put((url, content))
                        try_num = retries + 1
                    except Exception as e:
                        logger.error(f"Error downloading page {url}: {e}")
                        try_num += 1
                        # on_download_failure(url)
            await asyncio.sleep(check_every)
        else:
            empty_count += 1
    logger.info(f"Completed processing {url}")
    return


async def parse_page(parse_queue: Queue):
    """
    Parse the content of a page, extract urls.
    Pass extracted links back into queue for download
    """
    links = []
    parser = Parser(manager, rdb)
    check_every = 0.5
    empty_count = 0
    max_empty_count = 25
    while empty_count <= max_empty_count:
        while not parse_queue.empty():
            empty_count = 0
            while True:
                url, content = await parse_queue.get()
                print(f"Content received for {url} ")
                link_list = parser.get_links_from_content(
                    url, content
                )  # .get_links(content)
                for link in link_list:
                    links.append(link)
                    print(f"Link found: {link}")
                    add_page_to_visit(link)
                parse_queue.task_done()
        logger.info("Queue empty, sleeping before checking again")
        await asyncio.sleep(check_every)
        empty_count += 1
    logger.info("Completed parsing")
    return links


async def process_url_while_true(url: str, retries: int):
    parse_queue = Queue(20)
    add_page_to_visit(url)

    download_producer = asyncio.create_task(
        download_url_while_true(parse_queue, url, retries)
    )
    parsed_results = parse_while_true(parse_queue)
    all_links = await asyncio.gather(download_producer, parsed_results)
    logger.info(f"Completed processing {url}")
    return all_links


async def download_url_while_true(parse_queue: Queue, url: str, retries: int):
    """
    Periodically check the queue for new items requested for download.
    If there are new items, download them and add them to the queue.
    If there are no new items, sleep for a short period of time and check again.
    """
    downloader = SiteDownloader(host="localhost", port=7777)
    check_every = 0.2
    empty_count = 0
    max_empty_count = 25
    while True and empty_count <= max_empty_count:
        url = await get_page_to_visit()
        if url is not None:
            if not is_page_visited(url):
                try_num = 0
                print(f"Content received for {url} ")
                while try_num <= retries:
                    try:
                        add_page_visited(url)
                        content, status = downloader.get_page_elements(url)
                        await parse_queue.put((url, content))
                        try_num = retries + 1
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
    logger.info(f"Completed processing {url}")
    return


async def parse_while_true(parse_queue: Queue):
    """
    Parse the content of a page, extract urls.
    Pass extracted links back into queue for download
    """
    links = []
    parser = Parser(rdb)
    while True:
        url, content = await parse_queue.get()
        print(f"Content received for {url} ")
        link_list = parser.get_links_from_content(url, content)  # .get_links(content)
        for link in link_list:
            links.append(link)
            print(f"{url=}, {link=}")
            add_page_to_visit(link)

        parse_queue.task_done()


def crawl(seed_url: str, max_pages: int, retries: int):
    # main()
    atexit.register(manager.shutdown)
    manager.seed_url = seed_url
    manager.max_pages = max_pages
    manager.retries = retries
    manager.db_manager.start_run(manager.run_id, seed_url, max_pages)
    links = asyncio.run(process_url_while_true(url=seed_url, retries=retries))
    print(links)


if __name__ == "__main__":
    crawl(seed_url="https://overstory.com", max_pages=10, retries=3)
