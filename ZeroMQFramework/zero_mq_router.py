from .zero_mq_base import ZeroMQBase
from .config import ZeroMQProtocol
from .utils import *
import zmq
import threading
import signal


class ZeroMQRouter(ZeroMQBase):
    def __init__(self, port: int, protocol: ZeroMQProtocol = ZeroMQProtocol.TCP, backend_port: int = 5556,
                 backend_protocol: ZeroMQProtocol = ZeroMQProtocol.TCP):
        super().__init__(port, protocol)
        self.backend_port = backend_port
        self.backend_protocol = backend_protocol
        self.running = True
        self.frontend = None
        self.backend = None

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

        frontend_connection_string = f"{self.protocol.value}://*:{self.port}"

        backend_connection_string = build_connection_string(True,self.backend_protocol, self.backend_port)
        print(backend_connection_string)
        # backend_connection_string = f"{self.backend_protocol.value}://*:{self.backend_port}"

        try:
            self.frontend.bind(frontend_connection_string)

            self.backend.bind(backend_connection_string)

            print(f"Router started and bound to frontend {frontend_connection_string} and backend {backend_connection_string}")

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
        self.context.term()
        print("Cleaned up ZeroMQ sockets and context.")
