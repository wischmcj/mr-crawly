from __future__ import annotations

from diagrams import Cluster, Diagram, Edge
from diagrams.onprem.database import SQLite
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.network import Internet
from diagrams.programming.flowchart import Database, Decision, Process

# Create a diagram with a title and direction
with Diagram("Crawler Process Flow", show=True, direction="LR"):
    # External components
    internet = Internet("Target Websites")
    redis_queue = Redis("URL Queue")
    sqlite_db = SQLite("Data Store")

    # Main process components
    with Cluster("Crawler Process"):
        # Initial components
        sitemap = Process("Sitemap\nChecker")
        robots = Decision("Robots.txt\nValidator")

        # Core components
        downloader = Process(
            "Downloader\n- Fetches pages\n- Handles retries\n- Rate limiting"
        )
        parser = Process("Parser\n- Extracts links\n- Processes content")

        # Management components
        manager = Process("Manager\n- Coordinates flow\n- Tracks state")
        cache = Database("Cache\n- Stores recent\ncontent")

    # Define the process flow
    internet >> Edge(label="1. Check\nrobots.txt") >> robots
    robots >> Edge(label="2. Validate\npermissions") >> downloader

    # Sitemap flow
    internet >> Edge(label="3. Check\nsitemap") >> sitemap
    sitemap >> Edge(label="4. Add URLs") >> redis_queue

    # Main crawling flow
    redis_queue >> Edge(label="5. Get next\nURL") >> downloader
    downloader >> Edge(label="6. Store in\ncache") >> cache
    downloader >> Edge(label="7. Send\ncontent") >> parser

    # Parser flow
    parser >> Edge(label="8. Extract\nlinks") >> redis_queue
    parser >> Edge(label="9. Store\nrelationships") >> sqlite_db

    # Manager coordination
    manager >> Edge(label="10. Track\nprogress") >> [downloader, parser]
    manager >> Edge(label="11. Update\nstate") >> sqlite_db
