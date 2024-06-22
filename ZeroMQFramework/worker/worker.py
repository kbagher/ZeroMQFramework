from typing import Callable, Any
from ..common.processing_base import ZeroMQProcessingBase
from ..helpers.config import *
import zmq
from ..helpers.utils import create_message, parse_message
from ..helpers.node_type import ZeroMQNodeType
from ..heartbeat.heartbeat_sender import ZeroMQHeartbeatSender
from ..heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
import signal
import threading
import uuid
from ..helpers.logger import logger


class ZeroMQWorker(ZeroMQProcessingBase, threading.Thread):
    def __init__(self, connection: ZeroMQConnection, handle_message: Callable[[dict], Any] = None,
                 context: zmq.Context = None, node_type: ZeroMQNodeType = ZeroMQNodeType.WORKER,
                 heartbeat_config: ZeroMQHeartbeatConfig = None):
        threading.Thread.__init__(self)
        self.connection = connection
        self.handle_message = handle_message
        self.shutdown_requested = False
        self.context = context or zmq.Context()  # Use provided context (for worker manager) or create a new one for
        self.node_type = node_type

        # Server or a worker. 'DEALER' (worker) or 'REP' (server)
        self.socket = self.context.socket(zmq.DEALER if self.node_type == ZeroMQNodeType.WORKER else zmq.REP)

        # Random ID (not used in server but won't make any difference)
        self.worker_id = uuid.uuid4().__str__()
        self.socket.setsockopt(zmq.IDENTITY, self.worker_id.encode('utf-8'))  # Set the worker ID

        self.daemon = True  # True = Makes the thread a daemon thread

        # Heartbeat
        self.heartbeat_config = heartbeat_config
        self.heartbeat_enabled = self.heartbeat_config is not None

        # Sender heartbeat if it is a worker
        if self.heartbeat_enabled and self.node_type == ZeroMQNodeType.WORKER:
            self.heartbeat_sender = ZeroMQHeartbeatSender(context=self.context,
                                                          node_id=self.worker_id, node_type=ZeroMQNodeType.WORKER,
                                                          config=self.heartbeat_config)

        self.poll_timeout = 1000  # milliseconds

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, signum, frame):
        logger.warn(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_requested = True

    def run(self):
        self.start_worker()

    def start_worker(self):
        connection_string = self.connection.get_connection_string(bind=self.node_type is ZeroMQNodeType.SERVER)
        self.socket.bind(connection_string) if self.node_type == ZeroMQNodeType.SERVER else self.socket.connect(
            connection_string)
        if self.node_type == ZeroMQNodeType.SERVER:
            logger.info(f"{self.node_type.value} bind to {connection_string}")
        else:
            logger.info(f"{self.node_type.value} connected to {connection_string}")

        if self.heartbeat_enabled and self.node_type is ZeroMQNodeType.WORKER:
            self.heartbeat_sender.start()
        self.process_messages()

    def process_messages(self):
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        while not self.shutdown_requested:
            try:
                socks = dict(poller.poll(timeout=self.poll_timeout))
                if self.socket in socks:
                    message = self.socket.recv_multipart()
                    # print(message)
                    if self.node_type is ZeroMQNodeType.WORKER:  # worker mode
                        if len(message) < 4:
                            logger.error(f"Malformed message received: {message}")
                            continue
                        client_address = message[0]
                        parsed_message = parse_message(message[1:])
                        response = self.process_message(parsed_message)
                        if response:
                            self.socket.send_multipart([client_address, b''] + response)
                    elif self.node_type is ZeroMQNodeType.SERVER:  # Server mode
                        parsed_message = parse_message(message)
                        response = self.process_message(parsed_message)
                        if response:
                            self.socket.send_multipart(response)

            except zmq.ZMQError as e:
                logger.error(f"ZMQ Error occurred: {e}")
            except Exception as e:
                logger.error(f"Unknown exception occurred: {e}")

        # Exited the loop (self.shutdown_requested is true)
        self.cleanup(poller)

    def process_message(self, parsed_message: dict) -> list:
        # print(f"Processing message: {parsed_message}")
        if self.handle_message:
            response_data = self.handle_message(parsed_message)
        else:
            response_data = self.handle_message(parsed_message)
        msg = create_message(parsed_message["event_name"], response_data)
        return msg

    def cleanup(self, poller):
        logger.info("Worker is shutting down, performing cleanup...")
        if self.heartbeat_enabled:
            logger.info("Worker is calling stop heartbeat...")
            self.heartbeat_sender.stop()  # Wait for the heartbeat thread to stop
        poller.unregister(self.socket)
        self.socket.close()
        self.context.term()

    def handle_message(self, message: dict) -> Any:
        """To be implemented by passing a function during initialization"""
        raise NotImplementedError("Subclasses must implement this method.")
