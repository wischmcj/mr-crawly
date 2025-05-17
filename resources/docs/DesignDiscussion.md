# Interview Notes

# Project Planning
## Success Metrics
- Logically structured (reasonable benefits for drawbacks)
- Concurrency used effectively
- Extensibility (e.g. ability to make updates during interview)

## High-Level Requirements
1. Seed url to be provided via command line
2. Crawls only internal links
3. No use of web crawling frameworks (scrapy, crawlee)
4. Use concurrency as appropriate
5. *Written as a production piece of code

## Assumptions
 1. If provided a sub-domain, we should limit the search to said sub-domain
 2. Persistence of some sort is desirable for fault tolerance/error handling
 3. Being a CLI tool, this will be a utility that might be used on on a personal computer. That said, memory/cpu cores may be limited.
 4. Users will be somewhat tech-literate, and able to readily install external tools if provided with a link to set-up instructions
 5. The core functionality needed is the ability to scrape webpage links from a website, given this enables users to understand the scope of data available.

## Unknowns
These are more for brainstorming purposes than anything else.
- Purpose
  - What data is needed from these sites (e.g. are images needed? downloadable data sets?)
  - Are periodic updates needed to the stored html docs?
- Scale
  - What size of website, how many websites, etc. should the process be able to store data for?
- Latency
  - What constraints will there be regarding turn around time?
- Users/Usage
  - Given how the tool will be used and by whom, what format is preferable for the data produced by this tool? (e.g. csv, sqldb, ...)

# Design Decisions
## Data Flow/Storage
- A sqlite db file is created for each run; We write urls (representing the 'work items' of our core event loop) to this table in batches,
  - Reasoning: As per assumption 2, this allows from a run to be accessed even in the event of a failure. Likewise, as opposed to the redis database, this surfaces well structured data for analysis/review.
- A redis server is run concurrently with the application. This supports:
  - the UrlCache class - a cache for html content
    - Reasoning: This is a key component of our architecture, allowing for asynchronous downloading and parsing of web pages
    - This is likewise persisted, and can be useful in reducing the number of calls made to websites
  - the CrawlTracker class - a pubsub based 'backfeed' mechanism
    - Reasoning: Allows for database buffering while keeping updates to work items' (urls') status low-cost. Also provides a mechanism for sharing information across threads.

## 'Synchronicity', concurrency and threading
- Prior to running the core event loop, asyncronus pre-processing step looks for and processes the domain's sitemap (see the SiteMapper class).
  - Reasoning: As per assumption 3, it is beneficial to only store/ingest pertinent data. Larger sites such as google.com have a plethora of less than useful webpages that take up unwarranted space and processing time if not avoided. Sitemaps allow us to do so.
- The core event loop consists of a downloader (producer) and a parser (consumer). These are implemented using Asyncio Queues
  - Reasoning: This is naturally superior to any single threaded and synchronous alternatives, as it allows for download requests and parse requests to be processed as resources become available.
- The 'CrawlTracker' class functions on redis lists rather than using additional asyncio queues or a direct pubsub connection from the parser to the downloader
  - Reasoning: This creates a mechanism for controlling the speed at which new items are enterd into the queue, it also allows for concurrently running processes to be made aware of conditions within the main queue (See below)
- The UrlBulkWriter class is implemented as a pubsub subscriber running in a separate thread.
  - Reasoning: As useful as the sqlite db is, writes to its tables are expensive time-wise. Thread management adds a good deal of complexity, and is not worth the additional set-up cost in other areas of the program. However, enabling non-blocking writes to the database is well worth this added complexity.
- Only writes to the url table are implemented as discussed above.
  - Reasoning: The other tables in this process are only written to once or twice through the course of a run. The marginal time saved by encapsulating every db operation in a separate thread or threads isn't worth the cost.


## Overview
The CLI feeds user settings and the requested seed url to the crawling pipeline via main.py. A Manager instance facillitates the creation of our sqlite database, url cache and crawl tracker (e.g. work item manager). Sitemap urls are then collected and fed into to the'to_visit' redis list.
From here, our producer,  consumer and asyncio Queue handler the bulk of the 'crawling'. The producer downloads and caches html content and the consumer extracts requested data from said html content. The classed underlying these receive and write data via interaction with a centeralized Manager class.

### Key Classes
- Manager
  - Handles setup and teardown for all of the following classes:
    - DatabaseManager
      - Sets up/tears down sqlite table.
      - Provides an interface for interacting with those tables
        - In particular, this class batch writes finished urls to the DB
    - CrawlTracker
      - Manages the visited and to_visit lists
      - Surfaces only not-yet-visited links to the downloader
      - Once the maximum number of visits (e.g. the max number of pages parsed) has been reached, an instance of this class alerts the concurrently running database batch writer as well as the main event loop, beginning the shutdown process
      - Given the significant overlap in resources need to interact with the url database, this class also handles the publishing of write events to the urls table handler
    - UrlCache
      - An interface for caching html data by url, used to pass data from downloader to parser
      - Contents are persisted in an .rdb file
  - Manager also handles the context management (that is the desemination of user input data) to the various components of the program

A Manager instance is the passed to each of the following, allowing for database, cache and context access:
- SiteMapper
  - Runs prior to queueing
  - Retrieves the sitemap if possible, and traverses it
  - Adds links on pages referenced by the sitemap(s) to the 'to_visit' queue
- SiteDownloader
  - Implements politeness logic a la robots.txt
  - Downloads html given a url, caches that url data to redis.
  - puts urls that are ready for parsing to the asyncio queue
- Parser
  - Awaits work items (source urls) via the asyncio queue.
  - Extracts elements (currently just 'a' tags) from the pages downloaded by Downloader
  - Pushes the link urls found in each source url's html to the 'to_visit' queue via CrawlTracker
- CrawlTracker
  - Handles status updates on success and failure of downloader and parser runs
    - Primarily, this means preparing rows for, and emitting rows to the url table baching handler (UrlBulkWriter)
  - Also serves as a back feed mechanism - feeding the urls found by the parser back into the download queue
