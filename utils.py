"""
Utility functions for API calls and logging a heartbeat.
"""

import time
from mbu_dev_shared_components.database.logging import log_event
from config import HEARTBEAT_INTERVAL, LOG_DB


def log_heartbeat(stop_event):
    """
    Logs a heartbeat to indicate the service is running.

    Args:
        stop_event (multiprocessing.Event): Event to signal process termination.
    """
    while not stop_event.is_set():
        log_event(LOG_DB, "Service is running.", "INFO", context="Heartbeat")
        time.sleep(HEARTBEAT_INTERVAL)
