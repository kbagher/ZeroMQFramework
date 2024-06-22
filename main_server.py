from ZeroMQFramework import *
from typing import Any
import sys
import signal


def handle_message(message: dict) -> Any:
    return message


def main():
    logger.configure_logger('logs/server_logs')

    # Define the connection
    connection = ZeroMQTCPConnection(port=5555)
    ipc_path = "/tmp/my_super_app.ipc"  # IPC path, make sure it's unique for each application.
    # connection = ZeroMQIPCConnection(ipc_path=ipc_path)

    # Create the worker in REP mode
    worker = ZeroMQWorker(connection=connection, handle_message=handle_message, node_type=ZeroMQNodeType.SERVER)

    # Start the worker thread
    worker.start()

    # Register signal handlers to clean up on termination
    def cleanup(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        worker.request_shutdown(signum, frame)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    # Wait for the worker to finish
    worker.join()


if __name__ == "__main__":
    main()
