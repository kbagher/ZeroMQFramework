from .config import *
from .zero_mq_routing_strategy import *
from .zero_mq_heartbeat_handler import *
import zmq
import threading
import signal


class ZeroMQRouter:
    def __init__(self, frontend_connection: ZeroMQConnection, backend_connection: ZeroMQConnection,
                 strategy: Optional[ZeroMQRoutingStrategy] = None, heartbeat_enabled: bool = False, heartbeat_interval: int = 10, heartbeat_timeout: int = 30, max_missed: int = 3):
        self.frontend_connection = frontend_connection
        self.backend_connection = backend_connection
        self.running = True
        self.frontend = None
        self.backend = None
        self.context = zmq.Context()
        self.strategy = strategy
        self.heartbeat_enabled = heartbeat_enabled

        if self.heartbeat_enabled:
            self.heartbeat_handler = ZeroMQHeartbeatHandler(interval=heartbeat_interval, timeout=heartbeat_timeout, max_missed=max_missed)
        else:
            self.heartbeat_handler = None

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

            print(f"Router started and bound to frontend {frontend_connection_string} and backend {backend_connection_string}")

            proxy_thread = threading.Thread(target=self._start_proxy)
            proxy_thread.start()

            if self.heartbeat_enabled:
                self.heartbeat_handler.start()

            proxy_thread.join()

        except zmq.ZMQError as e:
            print(f"ZMQ Error occurred: {e}")
        except Exception as e:
            print(f"Unknown exception occurred: {e}")
        finally:
            print("Router is stopping...")
            if self.heartbeat_enabled:
                self.heartbeat_handler.stop()
            self.cleanup()

    def _start_proxy(self):
        poller = zmq.Poller()
        poller.register(self.frontend, zmq.POLLIN)
        poller.register(self.backend, zmq.POLLIN)

        while self.running:
            socks = dict(poller.poll())

            if self.frontend in socks and socks[self.frontend] == zmq.POLLIN:
                message = self.frontend.recv_multipart()
                self.backend.send_multipart(message)

            if self.backend in socks and socks[self.backend] == zmq.POLLIN:
                message = self.backend.recv_multipart()
                if self.is_heartbeat(message):
                    self.heartbeat_handler.handle_heartbeat(message[1])
                else:
                    if self.strategy:
                        self.strategy.route(self.frontend, self.backend)
                    else:
                        self.frontend.send_multipart(message)

    def is_heartbeat(self, message):
        return message[0] == b'HEARTBEAT'

    def cleanup(self):
        if self.frontend:
            self.frontend.close()
        if self.backend:
            self.backend.close()
        self.context.term()

        print("Cleaned up ZeroMQ sockets and context.")
