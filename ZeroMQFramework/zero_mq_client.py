import zmq
import signal
import time
from .utils import *
from .config import *


class ZeroMQClientError(Exception):
    pass


class ZeroMQConnectionError(ZeroMQClientError):
    pass


class ZeroMQTimeoutError(ZeroMQClientError):
    pass


class ZeroMQClient:
    def __init__(self, port: int, host: str = 'localhost', protocol: ZeroMQProtocol = ZeroMQProtocol.TCP,
                 timeout: int = 5000,
                 retry_attempts: int = 3, retry_timeout: int = 1000):
        self.port = port
        self.protocol = protocol
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_timeout = retry_timeout
        self.context = zmq.Context()
        self.socket = None
        self.connected = False
        self.host = host

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def connect(self):
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)
        connection_string = f"{self.protocol.value}://{self.host}:{self.port}"
        self.socket.connect(connection_string)
        self.connected = True
        print("Client connected to server")

    def reconnect(self):
        attempts = 0
        while attempts < self.retry_attempts:
            try:
                self.connect()
                return
            except zmq.ZMQError as e:
                print(f"Reconnect attempt {attempts + 1}/{self.retry_attempts} failed: {e}")
                attempts += 1
                time.sleep(self.retry_timeout / 1000)
        raise ZeroMQConnectionError("Unable to reconnect after several attempts")

    def send_message(self, event_name: str, event_data: dict):
        if not self.connected:
            self.reconnect()

        message = create_message(event_name, event_data)
        attempts = 0
        while attempts < self.retry_attempts:
            try:
                self.socket.send_multipart(message)
                res = self.receive_message()
                return res
            except zmq.Again:
                print("No response received within the timeout period, retrying...")
                attempts += 1
                time.sleep(self.retry_timeout / 1000)
            except zmq.ZMQError as e:
                print(f"ZMQError occurred: {e}, reconnecting socket.")
                self.socket.close()
                self.reconnect()
        raise ZeroMQTimeoutError("Unable to send message after several attempts")

    def receive_message(self):
        try:
            reply = self.socket.recv_multipart()
            return parse_message(reply)
        except zmq.Again as e:
            raise ZeroMQTimeoutError(f"No response received within the timeout period {e}")

    def request_shutdown(self, signum, frame):
        print(f"Received signal {signum}, shutting down gracefully...")
        self.cleanup()

    def cleanup(self):
        if self.socket:
            self.socket.close()
        self.context.term()
        print("Cleaned up ZeroMQ sockets and context.")
