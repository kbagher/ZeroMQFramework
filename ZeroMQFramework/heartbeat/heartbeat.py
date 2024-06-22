import threading
import time
from abc import ABC, abstractmethod
from enum import Enum

from ..heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
from ..helpers.node_type import ZeroMQNodeType
from ..common.socket_monitor import ZeroMQSocketMonitor
import zmq
from ..helpers.logger import logger


class ZeroMQHeartbeatType(Enum):
    SENDER = "sender"
    RECEIVER = "receiver"


class ZeroMQHeartbeat(ABC):
    def __init__(self, context: zmq.Context, node_id: str, node_type: ZeroMQNodeType, config: ZeroMQHeartbeatConfig):
        self.context = context
        self.node_id = node_id
        self.node_type = node_type
        self.config = config
        self.running = True
        self.socket = self.context.socket(self.get_socket_type())
        self.socket_monitor = ZeroMQSocketMonitor(context, self.socket)
        self.heartbeat_thread = None

    def get_socket_type(self):
        raise NotImplementedError("Subclasses must implement get_socket_type method")

    def get_heartbeat_type(self):
        raise NotImplementedError("Subclasses must implement get_heartbeat_type method")

    def start(self):
        # Always use demon to avoid blocking the main app from exiting
        self.heartbeat_thread = threading.Thread(target=self._run, daemon=True)
        self.heartbeat_thread.start()

    def connect(self, bind=False):
        while self.running:
            try:
                connection_string = self.config.connection.get_connection_string(bind)
                # Always start the monitor before connecting with the socket.
                # This ensures that you capture the initial events
                # I use monitor on sender only as the senders will send the heartbeat and will know if the remote node is up or down
                if self.get_heartbeat_type() is ZeroMQHeartbeatType.SENDER:
                    self.socket_monitor.start()  # Start the monitor after connecting
                if bind:
                    self.socket.bind(connection_string)
                    logger.info(f'Heartbeat reciever bound successfully. {connection_string}')
                else:
                    self.socket.connect(connection_string)
                    logger.info(f'Heartbeat sender connected successfully. {connection_string}')
                break
            except zmq.ZMQError as e:
                logger.error(f"ZMQ Error occurred during connect: ", e)
                time.sleep(self.config.interval)
                self.socket.close()
                self.socket = self.context.socket(self.get_socket_type())
            except Exception as e:
                logger.error(f"Unknown exception occurred during connect: ", e)
                time.sleep(self.config.interval)
                self.socket.close()
                self.socket = self.context.socket(self.get_socket_type())

    def stop(self):
        logger.info("Stopping heartbeat")
        self.running = False
        if self.heartbeat_thread is not None:
            self.heartbeat_thread.join()
        self.cleanup()

    def cleanup(self):
        if self.socket_monitor is not None:
            self.socket_monitor.stop()
        if self.socket is not None:
            self.socket.close()

    @abstractmethod
    def _run(self):
        raise NotImplementedError("Subclasses must implement _run method")
