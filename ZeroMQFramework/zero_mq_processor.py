from .zero_mq_processing_base import ZeroMQProcessingBase
from .config import ZeroMQProtocol
import zmq
from .utils import create_message, parse_message
import signal
import threading


class ZeroMQProcessor(ZeroMQProcessingBase):
    def __init__(self, port: int, protocol: ZeroMQProtocol = ZeroMQProtocol.TCP, address: str = "localhost",
                 mode: str = "worker"):
        super().__init__(port, protocol)
        self.address = address
        self.mode = mode
        self.shutdown_requested = False
        self.local = threading.local()

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

        if self.mode == "server":
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self._build_connection_string(bind=True))
        elif self.mode == "worker":
            self.socket = self.context.socket(zmq.DEALER)
            self.socket.connect(self._build_connection_string(bind=False))
        else:
            raise ValueError("Invalid mode. Choose either 'worker' or 'server'.")

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
                    print(f"{self.mode.capitalize()} received message: {message}")

                    if len(message) < 4:
                        print(f"Malformed message received: {message}")
                        continue

                    client_address = message[0]
                    parsed_message = parse_message(message[1:])
                    response = self.process_message(parsed_message)
                    if response:
                        print(f"{self.mode.capitalize()} sending response: {response}")
                        self.socket.send_multipart([client_address, b''] + response)
            except zmq.ZMQError as e:
                print(f"ZMQ Error occurred: {e}")
            except Exception as e:
                print(f"Unknown exception occurred: {e}")

        self.cleanup(poller)

    def process_message(self, parsed_message: dict) -> list:
        print(f"Processing message: {parsed_message}")
        event_name = parsed_message["event_name"]
        event_data = parsed_message["event_data"]
        response_data = self.handle_message(parsed_message)
        msg = create_message(event_name, response_data)
        return msg

    def cleanup(self, poller):
        print(f"{self.mode.capitalize()} is shutting down, performing cleanup...")
        poller.unregister(self.socket)
        self.socket.close()
        self.stop()
