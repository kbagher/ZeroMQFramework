import threading
import traceback
import datetime
import sys
import os
import logging
from enum import Enum
from logging.handlers import QueueHandler, QueueListener
import queue


class LogMode(Enum):
    UPDATE = "update"
    NEWLINE = "newline"


class logger:
    enabled = True
    mutex = threading.Lock()
    prefix = ""
    last_message = LogMode.NEWLINE
    log_folder = None
    logger = None
    log_queue = queue.Queue()
    console_queue = queue.Queue()  # Separate queue for console logging
    listener = None
    worker_thread = None

    ANSI_CODES = {
        'black': "\033[30m",
        'red': "\033[31m",
        'green': "\033[32m",
        'yellow': "\033[33m",
        'reset': "\033[0m",
    }

    @classmethod
    def configure_logger(cls, log_folder):
        if not os.path.exists(log_folder):
            os.makedirs(log_folder)
        cls.log_folder = log_folder

        # Create a timestamp for the log file name
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file = os.path.join(log_folder, f'debug_{timestamp}.log')

        # Set up logger
        cls.logger = logging.getLogger('DebugLogger')
        cls.logger.setLevel(logging.DEBUG)

        # Use a standard FileHandler instead of TimedRotatingFileHandler
        handler = logging.FileHandler(log_file)
        handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        queue_handler = QueueHandler(cls.log_queue)
        cls.logger.addHandler(queue_handler)

        # Start a QueueListener to handle logging in a separate thread
        cls.listener = QueueListener(cls.log_queue, handler)
        cls.listener.start()

        # Start the worker thread
        cls.worker_thread = threading.Thread(target=cls._log_worker, daemon=True)
        cls.worker_thread.start()

    @classmethod
    def clean_old_logs(cls, log_folder):
        now = datetime.datetime.now()
        for filename in os.listdir(log_folder):
            file_path = os.path.join(log_folder, filename)
            if os.path.isfile(file_path):
                file_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                if (now - file_time).days > 7:
                    os.remove(file_path)

    @classmethod
    def info(cls, *messages, mode=LogMode.NEWLINE, force=False):
        cls._enqueue_log(cls.ANSI_CODES['black'], "INFO", *messages, mode=mode, force=force)

    @classmethod
    def warn(cls, *messages, mode=LogMode.NEWLINE, force=False):
        cls._enqueue_log(cls.ANSI_CODES['yellow'], "WARN", *messages, mode=mode, force=force)

    @classmethod
    def error(cls, *messages, mode=LogMode.NEWLINE, force=False):
        cls._enqueue_log(cls.ANSI_CODES['red'], "ERROR", *messages, mode=mode, force=force)

    @classmethod
    def _enqueue_log(cls, color_code, log_type, *messages, mode=LogMode.NEWLINE, force=False):
        if cls.enabled or force:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for message in messages:
                if isinstance(message, Exception) and mode == LogMode.NEWLINE:
                    message = f"{message}\n{traceback.format_exc()}"

                log_message = f"[{timestamp}] {cls.prefix} {log_type}: {message}"
                cls.console_queue.put((color_code, log_type, log_message, mode))

                # Log to file without color formatting
                if cls.logger:
                    if log_type == "INFO":
                        cls.logger.info(message)
                    elif log_type == "WARN":
                        cls.logger.warning(message)
                    elif log_type == "ERROR":
                        cls.logger.error(message)

    @classmethod
    def _log_worker(cls):
        while True:
            color_code, log_type, log_message, mode = cls.console_queue.get()
            cls._process_log(color_code, log_type, log_message, mode)

    @classmethod
    def _process_log(cls, color_code, log_type, log_message, mode):
        if mode == LogMode.UPDATE:
            sys.stdout.write('\r')
            sys.stdout.write(f"{color_code}{log_message}{cls.ANSI_CODES['reset']}")
            sys.stdout.flush()
            cls.last_message = LogMode.UPDATE
        elif mode == LogMode.NEWLINE:
            if cls.last_message == LogMode.UPDATE:
                sys.stdout.write("\n")
            sys.stdout.write(f"{color_code}{log_message}{cls.ANSI_CODES['reset']}\n")
            sys.stdout.flush()
            cls.last_message = LogMode.NEWLINE

    @classmethod
    def shutdown(cls):
        if cls.listener:
            cls.listener.stop()
        if cls.worker_thread:
            cls.console_queue.put((None, None, None, None))  # Signal to stop the worker thread
            cls.worker_thread.join()
