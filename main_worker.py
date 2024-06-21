from ZeroMQFramework import *
from typing import Any
import sys


def create_handle_message() -> Callable[[dict], Any]:
    def handle_message(message: dict) -> Any:
        # print(f"Custom handler received: {message}")
        return [message]
    return handle_message


def signal_handler(signal, frame):
    print("Main process received shutdown signal")
    sys.exit(0)


if __name__ == "__main__":
    # Use an IPC connection for the workers if they are running on the same machine as the router
    ipc_path = "/tmp/my_super_app.ipc"
    worker_connection = ZeroMQIPCConnection(ipc_path=ipc_path)

    # Heartbeat
    ipc_path = "/tmp/my_super_app_heartbeat.ipc"  # IPC path, make sure it's unique for each application.
    heartbeat_conn = ZeroMQIPCConnection(ipc_path=ipc_path)
    # heartbeat_config = ZeroMQHeartbeatConfig(heartbeat_conn, interval=5, timeout=20,
    #                                          max_missed=5)
    heartbeat_config = ZeroMQHeartbeatConfig(heartbeat_conn, interval=1)

    num_workers = 1  # Specify number of worker threads

    # Initialize and start the WorkerManager with the custom message handler
    manager = ZeroMQMultiThreadedWorkers(connection=worker_connection, num_workers=num_workers,
                                         handle_message_factory=create_handle_message, heartbeat_config=heartbeat_config)
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
