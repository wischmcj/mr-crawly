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
    - Note: there are many different configurable environment variables available, though this is the only required one. You can find these in the simpler_crawler/config/configuration.py file and (for direnv users) in the .envrc.dist file

6. Optionally, logging can be configured using the simpler_crawler/config/logging_config.yml file.


## Usage
The below is a quick start-up guide for running this project. The commands provided have been tested on Ubuntu, but analogous commands are available on any common OS.

1. Clone this repository (or, if provided a zip file, extract the files)
```bash
    python main.py https://example.com
```
2. Change your working directory to the cloned/extracted folder
```bash
   cd simple_crawler
```
3. Start your redis server *on port 7777*
```bash
   redis-server --port 7777
```
   - Note: if you have existing programs running on port 7777

4. Call the CLI as follows
```bash
   python3 simple_crawler/cli.py 'https://overstory.com'
```
5. The links scrapped from the website requested will be printed to the console. However, users may find it easier to access the results within the results database. The file 'data_conn.py' (contents copied below) demonstrates how this can be done. As shown below, your data should be saved in a directory under simpler_crawler named based on the date and time the program was run:
```bash
    db_file = "simple_crawler/data/2025_05_12_20_37_33/sqlite.db"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    data = cursor.execute("SELECT * FROM urls").fetchall()
```
6. Cached url content may also be accessed via a connection to the redis-server. A copy of said server data can be found in the 'dump.rdb' file saved at the close of the program. This file can be found in the same directory mentioned above.

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
