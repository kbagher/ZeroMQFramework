import uuid

import zmq

from ZeroMQFramework.helpers.config import *
from ZeroMQFramework.helpers.utils import *
from ZeroMQFramework.helpers.error import *
from ZeroMQFramework.heartbeat.heartbeat_sender import ZeroMQHeartbeatSender
from ZeroMQFramework.heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
from ZeroMQFramework.helpers.node_type import ZeroMQNodeType


class ZeroMQClient:
    def __init__(self, connection: ZeroMQConnection,
                 heartbeat_config: ZeroMQHeartbeatConfig = None, timeout: int = 5000,
                 retry_attempts: int = 3, retry_timeout: int = 1000):
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_timeout = retry_timeout
        self.context = zmq.Context()
        self.connection = connection
        self.socket = None
        self.connected = False
        self.poller = zmq.Poller()
        self.monitor = None
        self.shutdown = False
        self.node_type = ZeroMQNodeType.CLIENT
        self.heartbeat_started = False
        # Random ID (not used in server but won't make any difference)
        self.node_id = uuid.uuid4().__str__()

        # Heartbeat
        self.heartbeat_config = heartbeat_config
        self.heartbeat_enabled = self.heartbeat_config is not None

        # Sender heartbeat if it is a worker
        if self.heartbeat_enabled:
            self.heartbeat = ZeroMQHeartbeatSender(context=self.context,
                                                   node_id=self.node_id, node_type=ZeroMQNodeType.CLIENT,
                                                   config=self.heartbeat_config)
        else:
            self.heartbeat = None

    def connect(self):
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)
        self.socket.setsockopt(zmq.SNDTIMEO, self.timeout)
        self.socket.setsockopt(zmq.IDENTITY, self.node_id.encode('utf-8'))  # Set the worker ID

        connection_string = self.connection.get_connection_string(bind=False)
        self.socket.connect(connection_string)
        self.connected = True
        if self.heartbeat_enabled and not self.heartbeat_started:
            self.heartbeat.start()
            self.heartbeat_started = False

        logger.info(f'Client connected to node at {connection_string}')

    def reconnect(self):
        self.cleanup_socket()
        attempts = 0
        while attempts < self.retry_attempts:
            try:
                self.connect()
                return
            except zmq.ZMQError as e:
                logger.warning(f"Reconnect attempt {attempts + 1}/{self.retry_attempts} failed: {e}")
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
                logger.warning("No response received within the timeout period, retrying...")
                attempts += 1
                time.sleep(self.retry_timeout / 1000)
            except zmq.ZMQError as e:
                if e.errno == zmq.EFSM or e.errno == zmq.EAGAIN:
                    logger.error("Socket is in an invalid state, reconnecting socket.")
                    self.reconnect()
                else:
                    logger.error(f"ZMQError occurred: {e}, reconnecting socket.")
                    self.reconnect()
        raise ZeroMQTimeoutError("Unable to send message after several attempts")

    def receive_message(self):
        reply = self.socket.recv_multipart()
        return parse_message(reply)

    def request_shutdown(self, signum, frame):
        logger.warning(f"Client Received signal {signum}, shutting down gracefully...")
        self.shutdown = True

    def cleanup_socket(self):
        logger.info("Cleaning up socket")
        if self.socket:
            logger.info("Closing socket")
            self.socket.close()
            self.connected = False
            logger.info("Socket closed")

    def cleanup(self):
        logger.info("Cleaning up client...")
        if self.monitor:
            logger.info("Cleaning up monitor")
            self.monitor.stop()
            logger.info("Monitor Cleaned")
        if self.heartbeat_enabled:
            logger.info("Client is calling stop heartbeat...")
            self.heartbeat.stop()  # Wait for the heartbeat thread to stop
        self.cleanup_socket()
        logger.info("Socket cleaned up")
        logger.info("Terminating context")
        self.context.term()
        logger.info("Cleaned up ZeroMQ sockets and context.")
