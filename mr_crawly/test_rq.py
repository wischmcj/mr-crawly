from __future__ import annotations
from abc import abstractmethod
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import redis
from rq import Queue, Worker
from utils import add
import logging

# cwd = os.getcwd()
# loc = os.path.dirname(__file__)
# print(loc)
# sys.path.append(loc)

from config.configuration import get_logger

logger = get_logger(__name__)


class BaseWorkClass(Worker):
    def __init__(
            self,
            *args,
            worker_class='base',
            host="localhost",
            port=7777,
            burst=False,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port
        self.work_class = worker_class # I could probably use actual class here ...
        self.continue_on_failure = False

    # @abstractmethod
    def on_success(job, connection, result):
        logger.info(f"download {job.args[0]} succeeded")
        connection.sadd('success', 'Base')
        pass

    def on_failure(self, url):
        """Callback for when a job succeeds"""
        logger.info(f"{self.work_class} for {url} failed")

    def work(self):
        super().work()


r = redis.Redis(host="localhost", port=7777, decode_responses=False)


def start_worker(queue_name, redis_conn):
    worker = BaseWorkClass(connection=redis_conn, queues=[queue_name],  burst=True)
    worker.work()


def start_workers(redis_conn, queue_name = 'test'):
    """
    Start rq workers for each queue.
    Workers can be accessed via our redis connection object, so
    we don't save pointers to the workers in the manager.
    """
    # q = Queue(connection=redis_conn, name=name)
    logger.info(f"Starting worker {queue_name}")
    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(start_worker, queue_name, redis_conn)
    # start_worker(queue_name, redis_conn,burst=True)


def main():
    q = Queue(connection=r, name="test")
    q.enqueue(add, args=(1, 2, 3), on_success=BaseWorkClass.on_success)
    print(q.jobs)
    start_workers(r)
    # _start_workers(q, r)
    time.sleep(10)
    breakpoint()


if __name__ == "__main__":
    main()
