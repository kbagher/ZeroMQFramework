from .config import *
import zmq
import threading
import signal


class ZeroMQRouter:
    def __init__(self, frontend_connection: ZeroMQConnection, backend_connection: ZeroMQConnection):
        self.frontend_connection = frontend_connection
        self.backend_connection = backend_connection
        self.running = True
        self.frontend = None
        self.backend = None
        self.context = zmq.Context()

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, signum, frame):
        print(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        self.cleanup()

    def start(self):
        context = self.context
        self.frontend = context.socket(zmq.ROUTER)
        self.backend = context.socket(zmq.DEALER)

        frontend_connection_string = self.frontend_connection.get_connection_string(bind=True)
        backend_connection_string = self.backend_connection.get_connection_string(bind=True)

        try:
            self.frontend.bind(frontend_connection_string)

            self.backend.bind(backend_connection_string)

            print(
                f"Router started and bound to frontend {frontend_connection_string}"
                f"and backend {backend_connection_string}")

            proxy_thread = threading.Thread(target=self._start_proxy)
            proxy_thread.start()
            proxy_thread.join()

        except zmq.ZMQError as e:
            print(f"ZMQ Error occurred: {e}")
        except Exception as e:
            print(f"Unknown exception occurred: {e}")
        finally:
            print("Router is stopping...")
            self.cleanup()

    def _start_proxy(self):
        try:
            zmq.proxy(self.frontend, self.backend)
        except zmq.ZMQError as e:
            print(f"ZMQ Proxy Error occurred: {e}")
        finally:
            print("Proxy is stopping...")

    def cleanup(self):
        if self.frontend:
            self.frontend.close()
        if self.backend:
            self.backend.close()
        self.executor.shutdown(wait=True)
        self.context.term()

        print("Cleaned up ZeroMQ sockets and context.")
