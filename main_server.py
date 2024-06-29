from ZeroMQFramework import *
from typing import Any
import sys
import signal


def handle_message(message: dict) -> Any:
    # print(message)
    return message


def main():
    setup_logging('logs/server_logs')
    config_file = 'config.ini'

    # server_config = load_config('config.ini', 'Server')
    # server_host = server_config['host']
    # server_port = server_config.getint('server_port')
    # server_heartbeat_port = server_config.getint('server_heartbeat_port')

    # server_config = load_config('config.ini', 'Server')
    config = load_config(config_file, "general")
    node_id = config.get('server_host')

    server_host = config.get('server_host')
    server_port = config.get('server_port')
    server_heartbeat_port = config.get('server_heartbeat_port')
    server_heartbeat_host = config.get('server_heartbeat_host')


    # Define the connection
    connection = ZeroMQTCPConnection(port=server_port, host=server_host)
    ipc_path = "/tmp/my_super_app.ipc"  # IPC path, make sure it's unique for each application.
    # connection = ZeroMQIPCConnection(ipc_path=ipc_path)

    ipc_path = "/tmp/my_super_app_heartbeat.ipc"  # IPC path, make sure it's unique for each application.
    # heartbeat_conn = ZeroMQIPCConnection(ipc_path=ipc_path)
    heartbeat_conn = ZeroMQTCPConnection(port=server_heartbeat_port, host=server_heartbeat_host)
    heartbeat_config = ZeroMQHeartbeatConfig(heartbeat_conn, interval=1)

    # Create the worker in REP mode
    worker = ZeroMQWorker(config_file=config_file, connection=connection, handle_message=handle_message,
                          heartbeat_config=heartbeat_config,
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
