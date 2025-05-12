from __future__ import annotations

import os
import time
from urllib.parse import urlparse

import redis



from config.configuration import get_logger

logger = get_logger(__name__)

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

from rq import Queue
class MyClass:
    myconn = redis.Redis(host='localhost', port=7777, db=0)
    def on_success(job, connection, result):
        """Callback for when a job succeeds"""
        myqueue = Queue(connection=myconn)
        logger.error(f"{job.meta} succeeded")
        myqueue.set('test', 'test')
        return 'test'



def add(a, b, sleep=0):
    """Add two numbers"""
    time.sleep(sleep)
    return a + b
