from ZeroMQFramework import *
from typing import Any
import sys
import signal


def handle_message(message: dict) -> Any:
    # print(message)
    return message


def main():
    setup_logging('logs/server_logs')


    # Define the connection
    connection = ZeroMQTCPConnection(port=5555)
    ipc_path = "/tmp/my_super_app.ipc"  # IPC path, make sure it's unique for each application.
    # connection = ZeroMQIPCConnection(ipc_path=ipc_path)

    ipc_path = "/tmp/my_super_app_heartbeat.ipc"  # IPC path, make sure it's unique for each application.
    # heartbeat_conn = ZeroMQIPCConnection(ipc_path=ipc_path)
    heartbeat_conn = ZeroMQTCPConnection(port=5556)
    heartbeat_config = ZeroMQHeartbeatConfig(heartbeat_conn, interval=1)


    # Create the worker in REP mode
    worker = ZeroMQWorker(connection=connection, handle_message=handle_message, heartbeat_config=heartbeat_config,
                          node_type=ZeroMQNodeType.SERVER)

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
