import time
from .config import *
from .utils import *
from .zero_mq_error import *
from .zero_mq_socket_monitor import *


class ZeroMQClient:
    def __init__(self, port: int, host: str = 'localhost', protocol: ZeroMQProtocol = ZeroMQProtocol.TCP,
                 timeout: int = 5000, retry_attempts: int = 3, retry_timeout: int = 1000):
        self.port = port
        self.protocol = protocol
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_timeout = retry_timeout
        self.context = zmq.Context()
        self.socket = None
        self.connected = False
        self.host = host
        self.poller = zmq.Poller()
        self.monitor = None
        self.shutdown = False

    def connect(self):
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)
        self.socket.setsockopt(zmq.SNDTIMEO, self.timeout)
        connection_string = f"{self.protocol.value}://{self.host}:{self.port}"
        self.socket.connect(connection_string)
        self.connected = True

        # # Set up monitoring with a new INPROC connection
        # monitor_connection = ZeroMQINPROCConnection("monitor_socket")
        # self.monitor = SocketMonitor(self.context, monitor_connection)
        # monitor_url = self.monitor.setup_monitor()
        # self.socket.monitor(monitor_url, zmq.EVENT_ALL)
        # self.monitor.start()  # Run the monitor in a separate thread

        # print(self.monitor.get_current_state())
        #

        Debug.info("Client connected to server")

    def reconnect(self):
        self.cleanup_socket()
        attempts = 0
        while attempts < self.retry_attempts:
            try:
                self.connect()
                return
            except zmq.ZMQError as e:
                Debug.warn(f"Reconnect attempt {attempts + 1}/{self.retry_attempts} failed: {e}")
                attempts += 1
                time.sleep(self.retry_timeout / 1000)
        raise ZeroMQConnectionError("Unable to reconnect after several attempts")

    def send_message(self, event_name: str, event_data: dict):
        if not self.connected:
            self.reconnect()

        message = create_message(event_name, event_data)
        attempts = 0
        while attempts < self.retry_attempts and not self.shutdown:
            try:
                self.socket.send_multipart(message)
                res = self.receive_message()
                return res
            except zmq.Again as e:
                Debug.warn("No response received within the timeout period, retrying...")
                attempts += 1
                time.sleep(self.retry_timeout / 1000)
            except zmq.ZMQError as e:
                if e.errno == zmq.EFSM or e.errno == zmq.EAGAIN:
                    Debug.error("Socket is in an invalid state, reconnecting socket.", e)
                    self.reconnect()
                else:
                    Debug.error(f"ZMQError occurred: {e}, reconnecting socket.", e)
                    self.reconnect()
        raise ZeroMQTimeoutError("Unable to send message after several attempts")

    def receive_message(self):
        reply = self.socket.recv_multipart()
        return parse_message(reply)

    def request_shutdown(self, signum, frame):
        Debug.warn(f"Client Received signal {signum}, shutting down gracefully...")
        self.shutdown = True


    def cleanup_socket(self):
        Debug.info("Cleaning up socket")
        if self.socket:
            Debug.info("Closing socket")
            self.socket.close()
            self.connected = False
            Debug.info("Socket closed")

    def cleanup(self):
        self.running = False
        Debug.info("Cleaning up client...")
        if self.monitor:
            Debug.info("Cleaning up monitor")
            self.monitor.stop()
            Debug.info("Monitor Cleaned")

        self.cleanup_socket()
        Debug.info("Socket cleaned up")
        Debug.info("Terminating context")
        self.context.term()
        Debug.info("Cleaned up ZeroMQ sockets and context.")
