# Mr. Crawly - Basic Web Crawler
<img src="docs/MrCrawly.png" width="400">

A somewhat-time-limited web-crawler exercise
A polite and efficient web crawler that respects robots.txt rules and implements rate limiting.

## Features

- Respects robots.txt rules
- Implements rate limiting to be polite to servers
- Only crawls URLs from the same domain as the starting URL
- Includes proper error handling and logging
- Command-line interface with configurable parameters

## Installation

1. Clone this repository
2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Basic usage:
```bash
python main.py https://example.com
```

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
