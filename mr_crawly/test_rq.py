from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import redis
from rq import Queue, Worker
from utils import add


def mycallback(job, connection, result, *args, **kwargs):
    print(f"Job {job.id} succeeded with result {result}")
    print(f"returned connection: {connection}")
    print(f"returned args: {args}")
    print(f"returned kwargs: {kwargs}")


def my_fail_callback(job, connection, result, *args, **kwargs):
    print(f"Job {job.id} failed with result {result}")
    print(f"returned connection: {connection}")
    print(f"returned args: {args}")
    print(f"returned kwargs: {kwargs}")


def start_worker(queue, redis_conn):
    worker = Worker(connection=redis_conn, queues=[queue.name])
    worker.work()


def _start_workers(queue, redis_conn):
    """
    Start rq workers for each queue.
    Workers can be accessed via our redis connection object, so
    we don't save pointers to the workers in the manager.
    """
    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(start_worker, queue, redis_conn)


def main():
    r = redis.Redis(host="localhost", port=7777, decode_responses=False)
    q = Queue(connection=r, name="test")
    q.enqueue(add, args=(1, 2), on_success=mycallback, on_failure=my_fail_callback)
    print(q.jobs)
    i = 0
    while i <= 10:
        _start_workers(q, r)
        i += 1
        time.sleep(1)
        print(f"Waiting for job to finish {i}")


if __name__ == "__main__":
    main()
