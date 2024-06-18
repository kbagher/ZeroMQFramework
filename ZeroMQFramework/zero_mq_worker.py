import json
from typing import Callable, Any
from .zero_mq_processing_base import ZeroMQProcessingBase
from .config import *
import zmq
from .utils import create_message, parse_message
import signal
import threading
import time
import uuid


class ZeroMQWorker(ZeroMQProcessingBase, threading.Thread):
    def __init__(self, connection: ZeroMQConnection, handle_message: Callable[[dict], Any] = None,
                 context: zmq.Context = None, heartbeat_interval: int = 0):
        threading.Thread.__init__(self)
        self.connection = connection
        self.handle_message = handle_message
        self.shutdown_requested = False
        self.context = context or zmq.Context()  # Use provided context (for worker manager) or create a new one for
        # standalone worker
        self.socket = self.context.socket(zmq.DEALER)
        self.worker_id = uuid.uuid4().__str__()
        # for each worker
        self.socket.setsockopt(zmq.IDENTITY, self.worker_id.encode('utf-8'))  # Set the worker ID
        self.daemon = True  # True = Makes the thread a daemon thread
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_thread = None

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, signum, frame):
        print(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_requested = True

    def run(self):
        self.start_worker()

    def start_worker(self):
        connection_string = self.connection.get_connection_string(bind=False)
        self.socket.connect(connection_string)
        print(f"Worker connected to {connection_string}")

        # Start heartbeat if enabled
        if self.heartbeat_interval > 0:
            self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
            self.heartbeat_thread.start()

        self.process_messages()

    def process_messages(self):
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        while not self.shutdown_requested:
            try:
                socks = dict(poller.poll(100))
                if self.socket in socks:
                    message = self.socket.recv_multipart()
                    # print(f"Worker received message: {message}")

                    if len(message) < 4:
                        print(f"Malformed message received: {message}")
                        continue

                    client_address = message[0]
                    parsed_message = parse_message(message[1:])
                    response = self.process_message(parsed_message)
                    if response:
                        self.socket.send_multipart([client_address, b''] + response)
            except zmq.ZMQError as e:
                print(f"ZMQ Error occurred: {e}")
            except Exception as e:
                print(f"Unknown exception occurred: {e}")

        # Exited the loop (self.shutdown_requested is true)
        self.cleanup(poller)

    def process_message(self, parsed_message: dict) -> list:
        print(f"Processing message: {parsed_message}")
        if self.handle_message:
            response_data = self.handle_message(parsed_message)
        else:
            response_data = self.handle_message(parsed_message)
        msg = create_message(parsed_message["event_name"], response_data)
        return msg

    def send_heartbeat(self):
        while not self.shutdown_requested:
            try:
                event_data = {"worker_id": self.worker_id}
                heartbeat_message = create_message('HEARTBEAT', event_data)
                self.socket.send_multipart(heartbeat_message)
                time.sleep(self.heartbeat_interval)
            except zmq.ZMQError as e:
                print(f"ZMQ Error occurred while sending heartbeat: {e}")
            except Exception as e:
                print(f"Unknown exception occurred while sending heartbeat: {e}")

    def cleanup(self, poller):
        print("Worker is shutting down, performing cleanup...")
        poller.unregister(self.socket)
        self.socket.close()
        self.executor.shutdown(wait=True)
        self.context.term()

    def handle_message(self, message: dict) -> Any:
        """To be implemented by passing a function during initialization"""
        raise NotImplementedError("Subclasses must implement this method.")
