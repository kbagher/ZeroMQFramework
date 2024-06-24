import configparser
import datetime
import json
import os
import sys
import time
import uuid

from loguru import logger
from concurrent.futures import ThreadPoolExecutor


def get_uuid_hex(length=32):
    """
    Generate a hexadecimal representation of a random UUID.

    :param length: The length of the hexadecimal representation. Default is 32.
    :return: A string representing the UUID in hexadecimal format.
    """
    return uuid.uuid4().hex[:length]


def get_uuid_str():
    """
    Generate a 32 character string representation of a random UUID

    :return: A string representation of a UUID.
    """
    return str(uuid.uuid4())


def get_current_time():
    """
    Get the current time in milliseconds.

    :return: The current time in milliseconds.
    :rtype: int
    """
    return int(time.time() * 1000)


def create_message(event_name: str, event_data: dict, include_empty_frame=False) -> list:
    try:
        message = [
            event_name.encode('utf-8'),  # Event Name
            json.dumps(event_data).encode('utf-8')  # Event Data
        ]
        if include_empty_frame:
            # Insert an empty frame at the beginning
            message.insert(0, b'')
        return message
    except Exception as e:
        raise ValueError(f"Error creating message for event {event_name} and data {event_data}: {e}")


def parse_message(message: list) -> dict:
    if len(message) < 2:
        raise ValueError(f"Malformed message: {message}")
    try:
        if message[0] == b'':  # Case: [empty frame, event name, event data]
            event_name = message[1].decode('utf-8')
            event_data = json.loads(message[2].decode('utf-8'))
        elif len(message) == 4 and message[1] == b'':  # Case: [address, empty frame, event name, event data]
            event_name = message[2].decode('utf-8')
            event_data = json.loads(message[3].decode('utf-8'))
        else:  # Case: [event name, event data]
            event_name = message[0].decode('utf-8')
            event_data = json.loads(message[1].decode('utf-8'))
        return {
            "event_name": event_name,
            "event_data": event_data
        }
    except Exception as e:
        raise ValueError(f"Error parsing message: {message}", e)


def load_config(config_file, section):
    config = configparser.ConfigParser()
    config.read(config_file)
    if section not in config:
        raise ValueError(f"Section {section} not found in the configuration file.")
    return config[section]


#####################
##### Used for logger

# Create a thread pool for logging
log_executor = ThreadPoolExecutor(max_workers=1)


def setup_logging(log_folder):
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    # Create a timestamp for the log file name
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_folder, f'debug_{timestamp}.log')

    logger.add(log_file, level="DEBUG", format="{time} - {level} - {message}")

    # Remove default stderr logger to customize format
    logger.remove(0)
    logger.add(sys.stderr, format="<green>{time}</green> - <level>{level}</level> - <level>{message}</level>")

    # Setup non-blocking logging using the thread pool
    logger.configure(patcher=patch_logging)

    # Optionally, if you want to keep old logs clean
    cleanup_old_logs(log_folder)


def patch_logging(record):
    log_executor.submit(logger.complete, record)


def cleanup_old_logs(log_folder):
    now = datetime.datetime.now()
    for filename in os.listdir(log_folder):
        file_path = os.path.join(log_folder, filename)
        if os.path.isfile(file_path):
            file_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            if (now - file_time).days > 7:
                os.remove(file_path)
