import zmq

from ZeroMQFramework import logger
from ZeroMQFramework.helpers.config import *
from ZeroMQFramework.helpers.utils import *
from ZeroMQFramework.helpers.error import *


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

        logger.info("Client connected to server")

    def reconnect(self):
        self.cleanup_socket()
        attempts = 0
        while attempts < self.retry_attempts:
            try:
                self.connect()
                return
            except zmq.ZMQError as e:
                logger.warn(f"Reconnect attempt {attempts + 1}/{self.retry_attempts} failed: {e}")
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
                logger.warn("No response received within the timeout period, retrying...")
                attempts += 1
                time.sleep(self.retry_timeout / 1000)
            except zmq.ZMQError as e:
                if e.errno == zmq.EFSM or e.errno == zmq.EAGAIN:
                    logger.error("Socket is in an invalid state, reconnecting socket.", e)
                    self.reconnect()
                else:
                    logger.error(f"ZMQError occurred: {e}, reconnecting socket.", e)
                    self.reconnect()
        raise ZeroMQTimeoutError("Unable to send message after several attempts")

    def receive_message(self):
        reply = self.socket.recv_multipart()
        return parse_message(reply)

    def request_shutdown(self, signum, frame):
        logger.warn(f"Client Received signal {signum}, shutting down gracefully...")
        self.shutdown = True


    def cleanup_socket(self):
        logger.info("Cleaning up socket")
        if self.socket:
            logger.info("Closing socket")
            self.socket.close()
            self.connected = False
            logger.info("Socket closed")

    def cleanup(self):
        self.running = False
        logger.info("Cleaning up client...")
        if self.monitor:
            logger.info("Cleaning up monitor")
            self.monitor.stop()
            logger.info("Monitor Cleaned")

        self.cleanup_socket()
        logger.info("Socket cleaned up")
        logger.info("Terminating context")
        self.context.term()
        logger.info("Cleaned up ZeroMQ sockets and context.")
