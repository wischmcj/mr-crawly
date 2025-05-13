# Simple Web Crawler
<img src="docs/MrCrawly.png" width="400">

A somewhat-time-limited web-crawler exercise
A polite and efficient web crawler that respects robots.txt rules and implements rate limiting.

## Features

- Respects robots.txt rules
- Implements rate limiting to be polite to servers
- Only crawls URLs from the same domain as the starting URL
- Includes proper error handling and logging
- Command-line interface with configurable parameters
- Persists data to a Sqlite DB and Redis server

## Prerequisites

## Installation

1. Clone this repository
2. Create a virtual environment (if you so choose)
    ```bash
        python3 -m venv venv
        source venv/bin/activate
    ```
3. Install the required python dependencies:
    ```bash
        pip3 install -r requirements.txt
    ```
4. Install the redis-server cli
   - [Windows Install Instructions](https://redis.io/docs/latest/operate/oss_and_stack/install/archive/install-redis/install-redis-on-windows/)
   - [Ubuntu Installation Instructions](https://redis.io/docs/latest/operate/oss_and_stack/install/archive/install-redis/install-redis-on-linux/)
   - [Mac](https://redis.io/docs/latest/operate/oss_and_stack/install/archive/install-redis/install-redis-on-mac-os/)
    **disclaimer - this tool was developed and tested on ubuntu.
5. Set the values of key configuration variable
    ```bash
    export SIMPLE_CRAWLER_LOG_CONFIG="<your-root-dir>/simple_crawler/simple_crawler/config/logging_config.yml"
    ```

## Usage
The below is a quick start-up guide for running this project. The commands provided have been tested on Ubuntu, but analogous commands are available on any common OS.

1. Clone this repository (or, if provided a zip file, extract the files)
```bash
    python main.py https://example.com
```
1. Change your working directory to the cloned/extracted folder
```bash
   cd simple_crawler
```
1. Start your redis server *on port 7777*
```bash
   redis-server --port 7777
```
   - Note: if you have existing programs running on port 7777

### Command Line Arguments

- `url` (required): The starting URL to crawl
- `--max-pages`: Maximum number of pages to crawl (default: 10)
- `--delay`: Delay between requests in seconds (default: 1.0)

### Examples

Crawl a website with default settings:
```bash
python main.py https://example.com
```

Crawl more pages with a longer delay between requests:
```bash
python main.py https://example.com --max-pages 20 --delay 2.0
```

## How It Works

The crawler:
1. Starts from the given URL
2. Checks robots.txt before crawling each page
3. Extracts all links from each page
4. Follows links within the same domain
5. Respects the specified delay between requests
6. Logs its progress to the console

## Polite Crawling

The crawler is designed to be polite to servers by:
- Respecting robots.txt rules
- Implementing rate limiting
- Only crawling the same domain
- Using proper user-agent headers
- Including error handling and timeouts

## Dependencies

- requests>=2.31.0
- beautifulsoup4>=4.12.0

## Workflow Tools
Due to [requirement 5](#high-level-requirements), more robust workflow tooling has been added than one might expect for a command line tool. Tools used:

- Pre-commit run, ruff powered linting
  - May catch small 'obvious' errors, but primarily is used for read ability
  - Makes the review process easier, enables future collaboration
- GitActions Workflows
  - Primarily, enables automated testing and eliminates manual work on the part of the developer
- Logging Configuration
  - Rich text formatting both improves UX for the CLI interface and the developer experience
  - File based logging enhances visibility into past runs
