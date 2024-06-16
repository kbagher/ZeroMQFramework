from ZeroMQFramework import *
from typing import Any
import sys


def handle_message(message: dict) -> Any:
    print(f"Custom handler received: {message}")
    return [message]


def signal_handler(signal, frame):
    print("Main process received shutdown signal")
    sys.exit(0)


if __name__ == "__main__":
    port = 5556  # Backend port
    address = "localhost"
    protocol = ZeroMQProtocol.IPC
    num_workers = 5  # Specify number of worker threads

    # Initialize and start the WorkerManager with the custom message handler
    manager = ZeroMQWorkerManager(port, protocol, address, num_workers, handle_message)
    manager.start()

    # Handle shutdown signals
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Keep the main thread alive to handle signals
    try:
        while True:
            signal.pause()
    except KeyboardInterrupt:
        pass
