"""
Defines a Windows service to journalize documents in GetOrganized.
"""

# General modules
import time
import sys

# Modules for service
import win32serviceutil
import win32service
import win32event
import servicemanager

# Modules for multiprocessing
import subprocess
import concurrent.futures
import threading

# MBU modules
from mbu_dev_shared_components.database.logging import log_event, log_heartbeat

# Config settings
from config import (
    FETCH_INTERVAL,
    SERVICE_CHECK_INTERVAL,
    LOG_DB,
    LOG_CONTEXT,
    HANDLE_FORMS,
    ENV,
    MAX_WORKERS,
    PATH_TO_PYTHONSERVICE
    )

# Main process
from process import main_process

# Helper modules
from case_manager import journalize_process as jp
from case_manager.helper_functions import fetch_cases_metadata


# Service framework
class JournalizeService(win32serviceutil.ServiceFramework):
    """Windows Service to journalize forms from database to GetOrganized periodically."""
    _svc_name_ = "JournalizeToGetOrganized"
    _svc_display_name_ = "Journalize to GetOrganized"
    _svc_description_ = "Windows service to journalize forms to GetOrganized."
    if not hasattr(sys, 'frozen'):
        _exe_name_ = PATH_TO_PYTHONSERVICE

    def __init__(self, args):
        """
        Initialize the service with given arguments.

        Args:
            args: Command-line arguments passed to the service.
        """
        super().__init__(args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.running = True
        self.processes = {}
        self.futures = []
        self.stop = False
        self.stop_event = threading.Event()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)

    def SvcStop(self):
        """
        Handle the stop signal for the service.

        This method is invoked when the service receives a stop request.
        It stops all running processes and sets the stop event.
        """
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, 0xF000, ("Service is stopping...", ""))
        log_event(LOG_DB, "INFO", "Service is stopping...", context=LOG_CONTEXT, db_env=ENV)
        self.running = False
        self.stop = True
        self.stop_event.set()

        # Wait for ongoing futures to finish
        ongoing_futures = [f for f in self.futures if f.running()]
        cancelled_futures = [f for f in self.futures if f.cancel()]
        if len(ongoing_futures) > 0:
            log_event(
                log_db=LOG_DB,
                level="INFO",
                message=f"Waiting {len(ongoing_futures)} for ongoing processes to finish. {len(cancelled_futures)} cancelled",
                context=LOG_CONTEXT,
                db_env=ENV,
            )
            while any(f.running() for f in self.futures):
                time.sleep(0.5)
        self.futures.clear()

        # Wait for ongoing sub processes to finish
        for name, process in self.processes.items():
            print(f"Trying to terminate {name}")
            try:
                process.terminate()
                print(f"{name} terminated")
            except Exception:
                print(f"Error when terminating {name}")
        self.processes.clear()

        # Log stopped heartbeat
        log_heartbeat(
            stop=self.stop,
            servicename=LOG_CONTEXT,
            heartbeat_interval=SERVICE_CHECK_INTERVAL,
            db_env=ENV,
        )

        # Log service stop
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE, 0xF000, ("Service stopped.", "")
        )
        log_event(LOG_DB, "INFO", "Service stopped.", context=LOG_CONTEXT, db_env=ENV)
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)

    def SvcDoRun(self):
        """
        Handle the start signal for the service.

        This method is invoked when the service receives a start request.
        It sets the service status to running and calls the main logic.
        """
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, 0xF000, ("Service is starting...", ""))
        log_event(
            LOG_DB, "INFO", "Service is starting...", context=LOG_CONTEXT, db_env=ENV
        )
        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
        try:
            self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        except Exception as e:
            servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, 0xF000, (f"Service encountered an error: {e}", ""))
            log_event(
                LOG_DB,
                "INFO",
                f"Service encountered an error: {e}",
                context=LOG_CONTEXT,
                db_env=ENV,
            )
            print(e)
        self.main()

    def worker(self, args):
        """Worker to initiate main process when no stop signal"""
        if not self.stop:
            main_process(*args)

    def main(self):
        """
        Main logic of the service.

        This method initializes the heartbeat process and periodically
        fetches new forms to journalize in GetOrganized.
        """
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, 0xF000, ("Service started.", ""))
        log_event(
            LOG_DB,
            "INFO",
            "Service started.",
            context=LOG_CONTEXT,
            db_env=ENV,
        )
        self.processes["heartbeat_process"] = subprocess.Popen(
            [
                "python",
                "-c",
                f"from mbu_dev_shared_components.database.logging import log_heartbeat; log_heartbeat('{self.stop}','{LOG_CONTEXT}','{SERVICE_CHECK_INTERVAL}','','{ENV}')",
            ]
        )

        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, 0xF000, ("Heartbeat process started.", ""))

        try:
            while self.running:
                log_event(
                    LOG_DB,
                    "INFO",
                    "Running journalize service.",
                    context=LOG_CONTEXT,
                    db_env=ENV,
                )
                # Fetch data from database
                credentials = jp.get_credentials_and_constants(db_env=ENV)  # Todo: copy constants and credentials from OO in prod SQL
                cases_metadata = fetch_cases_metadata(
                    connection_string=credentials["DbConnectionString"]  # When credential achieved from srv58, it is srv58.
                )
                # Fetch new forms
                log_event(
                    LOG_DB,
                    "INFO",
                    f"Checking new submissions for: {', '.join(HANDLE_FORMS)}",
                    LOG_CONTEXT,
                    ENV
                )
                forms_data = jp.get_forms_data(
                    conn_string=credentials["DbConnectionString"], params=HANDLE_FORMS
                )

                log_msg = (
                    f"Beginning journalizing process of {len(forms_data)} form(s)"
                    if len(forms_data) > 0
                    else "No forms to journalize.")
                log_event(
                    LOG_DB,
                    "INFO",
                    log_msg,
                    context=LOG_CONTEXT,
                    db_env=ENV,
                )

                # If no new forms, service sleeps and checks back after FETCH_INTERVAL (from config.py)
                if len(forms_data) == 0:
                    servicemanager.LogMsg(
                        servicemanager.EVENTLOG_INFORMATION_TYPE,
                        0xF000,
                        (
                            f"Now sleeping for {FETCH_INTERVAL // 60} minutes and {FETCH_INTERVAL % 60} seconds",
                            "",
                        ),
                    )

                    log_event(
                        LOG_DB,
                        "INFO",
                        f"Now sleeping for {FETCH_INTERVAL // 60} minutes and {FETCH_INTERVAL % 60} seconds",
                        context=LOG_CONTEXT,
                        db_env=ENV,
                    )
                    time.sleep(FETCH_INTERVAL)

                # If new forms initialize a ThreadPoolExecutor to handle forms concurrently (set MAX_WORKERS in config)
                else:
                    # We use a with statement to ensure threads are cleaned up promptly
                    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:  # Open parallel executor
                        future_to_worker = {
                            executor.submit(self.worker,  (form, credentials, cases_metadata, ENV)): form 
                            for form in forms_data
                        }
                        self.futures.extend(future_to_worker.keys())  # Add all futures to list of futures
                        for future in concurrent.futures.as_completed(future_to_worker):  # Loop over pending forms
                            form = future_to_worker[future]
                            try:
                                res = future.result()  # Checks for uncaught exceptions in the worker
                            except Exception as exc:
                                log_event(
                                    log_db=LOG_DB,
                                    level="ERROR",
                                    message=f"Form {form["form_id"]} failed somewhere: {exc}",
                                    context=LOG_CONTEXT,
                                    db_env=ENV
                                )
                        while not self.stop_event.is_set() and any(not f.done() for f in self.futures):
                            time.sleep(0.5)  # Checks for stop event or all tasks finished every half second

                    # Exiting ThreadPoolExecutor here
                    log_event(
                        log_db=LOG_DB,
                        level="INFO",
                        message="ThreadPoolExecutor exited cleanly",
                        context=LOG_CONTEXT,
                        db_env=ENV)
                    self.futures.clear()  # Clear list of futures when processes are finished

        except Exception as e:
            servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, 0xF000, (f"Service encountered an error: {e}", ""))
            self.SvcStop()

        finally:
            # Ensure all processes are terminated on exit
            self.SvcStop()


if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(JournalizeService)
