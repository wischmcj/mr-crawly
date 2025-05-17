from __future__ import annotations

import threading
from abc import abstractmethod

from config.configuration import get_logger

# currently no used, but its nice to have
# for debugging
logger = get_logger("helper_classes")


class BaseListener:
    """
    Subscribes to a Redis stream and performs an action
    on each message.
    """

    def __init__(self, pubsub, queue):
        """Initialize with a Redis pubsub object"""
        self.pubsub = pubsub
        self.queue = queue
        self.running = True

    def start(self):
        """Start the message handling loop in a separate thread"""
        self.thread = threading.Thread(target=self.handle_message)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        """Stop the message handling loop"""
        self.running = False
        self.thread.join()

    @abstractmethod
    def handle_message(self, message):
        """Handle a message"""
        pass

    @abstractmethod
    def flush(self):
        """Perform setup actions"""
        pass
