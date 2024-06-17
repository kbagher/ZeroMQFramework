from ZeroMQFramework import *


def main():
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

        # Initialize and start the router
        router = ZeroMQRouter(frontend_connection=frontend_conn, backend_connection=backend_conn)
        router.start()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
