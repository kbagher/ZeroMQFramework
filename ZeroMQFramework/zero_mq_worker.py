from .zero_mq_processing_base import ZeroMQProcessingBase
from .config import ZeroMQProtocol
import zmq
from .utils import create_message, parse_message
import signal
import threading


class ZeroMQWorker(ZeroMQProcessingBase):
    def __init__(self, port: int, protocol: ZeroMQProtocol = ZeroMQProtocol.TCP, address: str = "localhost", max_threads: int = 5):
        super().__init__(port, protocol, max_threads)
        self.address = address
        self.shutdown_requested = False

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, signum, frame):
        print(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_requested = True

    def start(self):
        self.socket = self.context.socket(zmq.DEALER)
        connection_string = f"{self.protocol}://{self.address}:{self.port}"
        self.socket.connect(connection_string)  # Connect to the router's backend
        print(f"Worker connected to {connection_string}")

        # self.send_registration()

        threads = []
        for _ in range(self.max_threads):
            thread = threading.Thread(target=self.process_messages)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def send_registration(self):
        print("Sending registration")
        registration_message = create_message("REGISTER", {})
        print(registration_message)
        self.socket.send_multipart(registration_message)  # Send registration message

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

    def send_deregister(self):
        deregister_message = create_message("DEREGISTER", {})
        self.socket.send_multipart(deregister_message)  # Send deregister message

    def process_message(self, parsed_message: dict) -> list:
        print(f"Processing message: {parsed_message}")
        event_name = parsed_message["event_name"]
        event_data = parsed_message["event_data"]
        response_data = self.handle_message(parsed_message)
        msg = create_message(event_name, response_data)
        # response_data = {
        #     "status": "processed",
        #     "data": event_data
        # }
        return msg
