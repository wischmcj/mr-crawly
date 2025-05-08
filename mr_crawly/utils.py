from __future__ import annotations

import os
from urllib.parse import urlparse


def parse_url(url: str):
    """Parse a url and return the page elements"""
    parsed_url = urlparse(url)
    return parsed_url.scheme, parsed_url.netloc, parsed_url.path


def create_dir(dir_name):
    # Method 1: Using os.mkdir() to create a single directory
    dir_name = "my_new_directory"
    try:
        os.mkdir(dir_name)
        print(f"Directory '{dir_name}' created successfully.")
    except FileExistsError:
        print(f"Directory '{dir_name}' already exists.")
    except PermissionError as err:
        print(f"Permission denied: Unable to create '{dir_name}'.")
        raise err
