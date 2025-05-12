from __future__ import annotations

import os
import sys

cwd = os.getcwd()

from simple_crawler.config.configuration import get_logger  # noqa

logger = get_logger(__name__)

# Read in environment variables, set defaults if not present
loc = os.path.join(os.path.dirname(__file__), "simple_crawler")
sys.path.append(loc)
