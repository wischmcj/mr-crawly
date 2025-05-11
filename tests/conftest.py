from __future__ import annotations

import os
import sys

cwd = os.getcwd()
sys.path.append(cwd + "/src/")

from simple_crawler.config.configuration import get_logger  # noqa

logger = get_logger(__name__)

cwd = os.getcwd()
# Read in environment variables, set defaults if not present
loc = os.path.dirname(__file__)
sys.path.append(cwd)
sys.path.append(os.path.dirname(cwd))
