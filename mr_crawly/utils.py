from __future__ import annotations

import os
from urllib.parse import urlparse

from config.configuration import get_logger

logger = get_logger(__name__)


def parse_url(url: str):
    """Parse a url and return the page elements"""
    parsed_url = urlparse(url)
    return parsed_url.scheme, parsed_url.netloc, parsed_url.path


def create_dir(dir_name, exist_ok=False):
    # Method 1: Using os.mkdir() to create a single directory
    try:
        os.mkdir(dir_name)
        logger.info(f"Directory '{dir_name}' created successfully.")
    except FileExistsError:
        if exist_ok:
            logger.info(f"Directory '{dir_name}' already exists.")
        else:
            raise FileExistsError(f"Directory '{dir_name}' already exists.")
    except PermissionError as err:
        logger.info(f"Permission denied: Unable to create '{dir_name}'.")
        raise err
