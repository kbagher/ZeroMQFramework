from ZeroMQFramework import *


def main():
    # Debug.configure_logger('logs/router_logs')

    try:
        # Create frontend TCP connection.
        # This is the connection that will be used with clients
        frontend_conn = ZeroMQTCPConnection(port=5555)

        # Create backend IPC connection
        # This is the connection that will be used with workers.
        # What o use:
        #   If workers are in the same server (running as processes), you can use IPC (ZeroMQIPCConnection)
        #   If workers are on different machines, you can use TCP (ZeroMQTCPConnection)
        ipc_path = "/tmp/my_super_app.ipc" # IPC path, make sure it's unique for each application.
        backend_conn = ZeroMQIPCConnection(ipc_path=ipc_path)

        # Heartbeat
        ipc_path = "/tmp/my_super_app_heartbeat.ipc"  # IPC path, make sure it's unique for each application.
        heartbeat_conn = ZeroMQIPCConnection(ipc_path=ipc_path)
        heartbeat_config = ZeroMQHeartbeatConfig(heartbeat_conn, interval=1, timeout=5, max_missed=1)

        # Initialize and start the router
        router = ZeroMQRouter(frontend_connection=frontend_conn, backend_connection=backend_conn,
                              heartbeat_config=heartbeat_config)
        router.start()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
