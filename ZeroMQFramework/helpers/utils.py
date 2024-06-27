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
            # Insert an empty frame at the beginning. This is used in some cases.
            # This is used in the heartbeat sender as the socket type is a dealer
            # Which causes the socket to add its id to the message, so we have to add an empty frame which allows
            # ZeroMQ to know that the first frame is the dealer's id
            # I'm not sure of this is a bug or not, but sometimes ZeroMQ does not add an empty frame.
            message.insert(0, b'')
        return message
    except Exception as e:
        raise ValueError(f"Error creating message for event {event_name} and data {event_data}: {e}")


def parse_message(message: list) -> dict:
    """
    Parse a message and return a dictionary containing the event_name and event_data.

    :param message: A list representing the message to parse.
    :return: A dictionary with the keys "event_name" and "event_data".
    :raises ValueError: If the message is malformed or cannot be parsed.
    """
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
    """
    Load the specified section of a configuration file.

    :param config_file: The path to the configuration file.
    :param section: The section name in the configuration file to load.
    :return: The specified section of the configuration file.
    :raises ValueError: If the specified section is not found in the configuration file.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    if section not in config:
        raise ValueError(f"Section {section} not found in the configuration file.")
    return config[section]


def save_config(config_file, section, key, value):
    """
    Save a key-value pair to a specified section of a configuration file.

    :param config_file: The path to the configuration file.
    :param section: The section name in the configuration file to save to.
    :param key: The key to save.
    :param value: The value to save.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    if section not in config:
        config.add_section(section)
    config.set(section, key, value)
    with open(config_file, 'w') as configfile:
        config.write(configfile)


#####################
##### Used for logger

# Create a thread pool for logging
log_executor = ThreadPoolExecutor(max_workers=1)


def setup_logging(log_folder):
    """
    :param log_folder: The folder to store the log files.
    :return: None

    This method sets up logging for the application. It creates the log folder if it does not already exist, creates a timestamp for the log file name, sets the log file path, adds the log file to the logger, removes the default stderr logger, configures non-blocking logging using a thread pool, and optionally cleans up old log files in the log folder.
    """
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    # Create a timestamp for the log file name
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_folder, f'debug_{timestamp}.log')

    logger.add(log_file, level="DEBUG", format="{time} - {level} - {name}:{line} - {message}")

    # Remove the default stderr logger
    logger.remove(0)

    # Add a logger for the console with a simpler format
    logger.add(sys.stderr, level="INFO", format="<green>{time}</green> - <level>{message}</level>")

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
