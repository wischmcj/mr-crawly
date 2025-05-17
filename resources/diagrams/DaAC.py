from __future__ import annotations

# Groups
# Manager
# DB Manager
# Tables
# Cache - Redis
# Crawl Tracker
# URL Cache
# Crawler
# Sitemap Checker
# Downloader
# Parser

# # Create a diagram with a title and direction
# with Diagram("Crawler Process Flow", show=True, direction="LR"):
#     # External components
#     internet = Internet("Target Websites")
#     redis_queue = Redis("URL Queue")
#     sqlite_db = Mysql("Data Store")

#     # Main process components
#     with Cluster("Crawler Process"):
#         # Initial components
#         sitemap = PredefinedProcess("Sitemap\nChecker")
#         robots = Decision("Robots.txt\nValidator")

#         # Core components
#         downloader = PredefinedProcess("Downloader\n- Fetches pages\n- Handles retries\n- Rate limiting")
#         parser = PredefinedProcess("Parser\n- Extracts links\n- Processes content")

#         # Management components
#         manager = PredefinedProcess("Manager\n- Coordinates flow\n- Tracks state")
#         cache = Database("Cache\n- Stores recent\ncontent")

#     # Define the process flow
#     internet >> Edge(label="1. Check\nrobots.txt") >> robots
#     robots >> Edge(label="2. Validate\npermissions") >> downloader

#     # Sitemap flow
#     internet >> Edge(label="3. Check\nsitemap") >> sitemap
#     sitemap >> Edge(label="4. Add URLs") >> redis_queue

#     # Main crawling flow
#     redis_queue >> Edge(label="5. Get next\nURL") >> downloader
#     downloader >> Edge(label="6. Store in\ncache") >> cache
#     downloader >> Edge(label="7. Send\ncontent") >> parser

#     # Parser flow
#     parser >> Edge(label="8. Extract\nlinks") >> redis_queue
#     parser >> Edge(label="9. Store\nrelationships") >> sqlite_db

#     # Manager coordination
#     manager >> Edge(label="10. Track\nprogress") >> [downloader, parser]
#     manager >> Edge(label="11. Update\nstate") >> sqlite_db
