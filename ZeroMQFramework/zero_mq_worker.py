from .zero_mq_processing_base import ZeroMQProcessingBase
from .config import ZeroMQProtocol
import zmq
from .utils import create_message, parse_message
import signal
import threading


class ZeroMQWorker(ZeroMQProcessingBase):
    def __init__(self, port: int, protocol: ZeroMQProtocol = ZeroMQProtocol.TCP, address: str = "localhost"):
        super().__init__(port, protocol)
        self.address = address
        self.shutdown_requested = False
        self.local = threading.local()

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, signum, frame):
        print(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_requested = True

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
                        print([client_address, b''] + response)
                        self.socket.send_multipart([client_address, b''] + response)
            except zmq.ZMQError as e:
                print(f"ZMQ Error occurred: {e}")
            except Exception as e:
                print(f"Unknown exception occurred: {e}")

    def process_message(self, parsed_message: dict) -> list:
        print(f"Processing message: {parsed_message}")
        event_name = parsed_message["event_name"]
        event_data = parsed_message["event_data"]
        response_data = self.handle_message(parsed_message)
        msg = create_message(event_name, response_data)
        return msg
