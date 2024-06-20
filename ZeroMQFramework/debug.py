import threading
import traceback
import datetime
import sys
from enum import Enum


class LogMode(Enum):
    UPDATE = "update"
    NEWLINE = "newline"


class Debug:
    # Determine if logging is enabled based on the environment
    # enabled = os.getenv('PYTHON_ENV') in ['staging', 'testing', 'development']
    enabled = True
    # Mutex for thread-safe logging
    mutex = threading.Lock()
    # Prefix for all log messages
    prefix = ""
    last_message = LogMode.NEWLINE

    # ANSI escape codes for coloring the console output
    ANSI_CODES = {
        'red': "\033[31m",
        'green': "\033[32m",
        'yellow': "\033[33m",
        'reset': "\033[0m",
    }

    @classmethod
    def info(cls, *messages, mode=LogMode.NEWLINE, force=False):
        cls._log(cls.ANSI_CODES['green'], "INFO", *messages, mode=mode, force=force)

    @classmethod
    def warn(cls, *messages, mode=LogMode.NEWLINE, force=False):
        cls._log(cls.ANSI_CODES['yellow'], "WARN", *messages, mode=mode, force=force)

    @classmethod
    def error(cls, *messages, mode=LogMode.NEWLINE, force=False):
        cls._log(cls.ANSI_CODES['red'], "ERROR", *messages, mode=mode, force=force)

    @classmethod
    def _log(cls, color_code, log_type, *messages, mode=LogMode.NEWLINE, force=False):
        if cls.enabled or force:
            with cls.mutex:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for message in messages:
                    if isinstance(message, Exception) and mode == LogMode.NEWLINE:
                        message = f"{message}\n{traceback.format_exc()}"

                    if mode == LogMode.UPDATE:
                        sys.stdout.write('\r')
                        sys.stdout.write(
                            f"{color_code}[{timestamp}] {cls.prefix} {log_type}: {message}{cls.ANSI_CODES['reset']}")
                        sys.stdout.flush()
                        cls.last_message = LogMode.UPDATE
                    elif mode == LogMode.NEWLINE:
                        if cls.last_message == LogMode.UPDATE:
                            sys.stdout.write("\n")
                        sys.stdout.write(
                            f"{color_code}[{timestamp}] {cls.prefix} {log_type}: {message}{cls.ANSI_CODES['reset']}\n")
                        sys.stdout.flush()
                        cls.last_message = LogMode.NEWLINE
