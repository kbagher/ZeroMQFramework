from typing import Callable, Any
from .zero_mq_processing_base import ZeroMQProcessingBase
from .config import ZeroMQProtocol
import zmq
from .utils import create_message, parse_message
import signal
import threading


class ZeroMQWorker(ZeroMQProcessingBase, threading.Thread):
    def __init__(self, port: int, protocol: ZeroMQProtocol = ZeroMQProtocol.TCP, address: str = "localhost", handle_message: Callable[[dict], Any] = None):
        ZeroMQProcessingBase.__init__(self, port, protocol)
        threading.Thread.__init__(self)
        self.address = address
        self.handle_message = handle_message
        self.shutdown_requested = False
        # self.protocol = protocol
        self.daemon = True  # Optional: makes the thread a daemon thread

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, signum, frame):
        print(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_requested = True

    def run(self):
        self.start_worker()

    def start_worker(self):
        self.socket = self.context.socket(zmq.DEALER)
        connection_string = self._build_connection_string(bind=False)
        print(connection_string)
        self.socket.connect(connection_string)
        print(f"Worker connected to {connection_string}")
        self.process_messages()

    def process_messages(self):
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        while not self.shutdown_requested:
            try:
                socks = dict(poller.poll(100))
                if self.socket in socks:
                    message = self.socket.recv_multipart()
                    print(f"Worker received message: {message}")

                    if len(message) < 4:
                        print(f"Malformed message received: {message}")
                        continue

                    client_address = message[0]
                    parsed_message = parse_message(message[1:])
                    response = self.process_message(parsed_message)
                    if response:
                        print(f"Worker sending response: {response}")
                        self.socket.send_multipart([client_address, b''] + response)
            except zmq.ZMQError as e:
                print(f"ZMQ Error occurred: {e}")
            except Exception as e:
                print(f"Unknown exception occurred: {e}")

        self.cleanup(poller)

    def process_message(self, parsed_message: dict) -> list:
        print(f"Processing message: {parsed_message}")
        if self.handle_message:
            response_data = self.handle_message(parsed_message)
        else:
            response_data = self.handle_message(parsed_message)
        msg = create_message(parsed_message["event_name"], response_data)
        return msg

    def cleanup(self, poller):
        print("Worker is shutting down, performing cleanup...")
        poller.unregister(self.socket)
        self.socket.close()
        self.stop()

    def handle_message(self, message: dict) -> Any:
        """To be implemented by passing a function during initialization"""
        raise NotImplementedError("Subclasses must implement this method.")
