from typing import Callable, Any
from ..common.processing_base import ZeroMQProcessingBase
from ZeroMQFramework.common.connection_protocol import *
import zmq
from ..common.base import ZeroMQBase
from ..helpers.utils import create_message, parse_message
from ZeroMQFramework.common.node_type import ZeroMQNodeType
from ..heartbeat.heartbeat_sender import ZeroMQHeartbeatSender
from ..heartbeat.heartbeat_receiver import ZeroMQHeartbeatReceiver
from ..heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
import signal
import threading
from ..helpers.utils import *
from loguru import logger


class ZeroMQWorker(ZeroMQBase, ZeroMQProcessingBase, threading.Thread):
    def __init__(self, config_file: str, connection: ZeroMQConnection, handle_message: Callable[[dict], Any] = None,
                 context: zmq.Context = None, node_type: ZeroMQNodeType = ZeroMQNodeType.WORKER,
                 heartbeat_config: ZeroMQHeartbeatConfig = None):
        super().__init__(config_file, connection, node_type, handle_message, context, heartbeat_config)


    def run(self):
        self.start_worker()

    def start_worker(self):
        connection_string = self.connection.get_connection_string(bind=self.node_type == ZeroMQNodeType.SERVER)
        if self.node_type == ZeroMQNodeType.SERVER:
            self.socket.bind(connection_string)
            logger.info(f"{self.node_type.value} bind to {connection_string}")
        else:
            self.socket.connect(connection_string)
            logger.info(f"{self.node_type.value} connected to {connection_string}")

        if self.heartbeat_enabled:
            self.heartbeat.start()
        self.process_messages()

    def process_messages(self):
        self.poller.register(self.socket, zmq.POLLIN)

        while not self.shutdown_requested:
            try:
                socks = dict(self.poller.poll(timeout=self.poller_timeout))
                if self.socket in socks:
                    message = self.socket.recv_multipart()
                    if self.node_type == ZeroMQNodeType.WORKER:  # worker mode
                        if len(message) < 4:
                            logger.error(f"Malformed message received: {message}")
                            continue
                        client_address = message[0]
                        parsed_message = parse_message(message[1:])
                        response = self.process_message(parsed_message)
                        if response:
                            self.socket.send_multipart([client_address, b''] + response)
                    elif self.node_type == ZeroMQNodeType.SERVER:  # Server mode
                        parsed_message = parse_message(message)
                        response = self.process_message(parsed_message)
                        if response:
                            self.socket.send_multipart(response)

            except zmq.ZMQError as e:
                logger.error(f"ZMQ Error occurred: {e}")
            except Exception as e:
                logger.error(f"Unknown exception occurred: {e}")

        # Exited the loop (self.shutdown_requested is true)
        self.cleanup()

    def process_message(self, parsed_message: dict) -> list:
        response_data = self.handle_message(parsed_message)
        msg = create_message(parsed_message["event_name"], response_data)
        return msg

    def cleanup(self):
        logger.info(f"{self.node_type.value} is shutting down, performing cleanup...")
        self.poller.unregister(self.socket)
        super().cleanup()

    def handle_message(self, message: dict) -> Any:
        """To be implemented by passing a function during initialization"""
        raise NotImplementedError("Subclasses must implement this method.")
