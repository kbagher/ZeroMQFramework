from .zero_mq_base import ZeroMQBase
from .config import ZeroMQProtocol
import zmq
from typing import List
from .utils import create_message, parse_message
import threading


class ZeroMQRouter(ZeroMQBase):
    def __init__(self, port: int, protocol: ZeroMQProtocol = ZeroMQProtocol.TCP, backend_port: int = 5556,
                 backend_protocol: ZeroMQProtocol = ZeroMQProtocol.TCP, max_threads: int = 5):
        super().__init__(port, protocol, max_threads)
        self.backend_port = backend_port
        self.backend_protocol = backend_protocol
        self.workers = []
        self.running = True
        self.frontend = None  # Initialize frontend attribute
        self.backend = None   # Initialize backend attribute

    def start(self):
        context = self.context
        self.frontend = context.socket(zmq.ROUTER)
        self.backend = context.socket(zmq.DEALER)

        frontend_connection_string = f"{self.protocol}://*:{self.port}"
        backend_connection_string = f"{self.backend_protocol.value}://*:{self.backend_port}"

        self.frontend.bind(frontend_connection_string)
        self.backend.bind(backend_connection_string)

        print(
            f"Router started and bound to frontend {frontend_connection_string} and backend {backend_connection_string}")

        frontend_poller = zmq.Poller()
        frontend_poller.register(self.frontend, zmq.POLLIN)

        backend_poller = zmq.Poller()
        backend_poller.register(self.backend, zmq.POLLIN)

        worker_thread = threading.Thread(target=self.start_backend, args=(self.backend,))
        worker_thread.start()

        try:
            while self.running:
                socks = dict(frontend_poller.poll(100))

                if self.frontend in socks:
                    message = self.frontend.recv_multipart()
                    print(f"Router received message from client: {message}")

                    if len(message) < 2:
                        print(f"Malformed message received: {message}")
                        continue

                    client_address = message[0]
                    event_name = message[2]
                    event_data = message[3]
                    self.backend.send_multipart([client_address, b'', event_name, event_data])
                    print(f"Forwarding message {message} to worker")

        except zmq.ZMQError as e:
            print(f"ZMQ Error occurred: {e}")
        except Exception as e:
            print(f"Unknown exception occurred: {e}")
            self.stop()
        # finally:
        #     print("Router is stopping...")

    def start_backend(self, backend):
        backend_poller = zmq.Poller()
        backend_poller.register(backend, zmq.POLLIN)

        try:
            while self.running:
                socks = dict(backend_poller.poll(100))

                if backend in socks:
                    message = backend.recv_multipart()
                    print(f"Router received message from worker: {message}")

                    if len(message) < 4:
                        print(f"Malformed message received: {message}")
                        continue

                    client_address = message[0]
                    event_name = message[2]
                    event_data = message[3]
                    self.frontend.send_multipart([client_address, b'', event_name, event_data])
                    print(f"Forwarding response to client: {client_address}")

        except zmq.ZMQError as e:
            print(f"ZMQ Error occurred: {e}")
        except Exception as e:
            print(f"Unknown exception occurred: {e}")
        finally:
            print("Backend router is stopping...")

    def register_worker(self, worker):
        if worker not in self.workers:
            self.workers.append(worker)

    def remove_worker(self, worker):
        if worker in self.workers:
            self.workers.remove(worker)
        print(f"Worker {worker} has been removed.")
