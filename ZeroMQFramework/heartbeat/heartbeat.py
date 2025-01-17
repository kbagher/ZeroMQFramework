import threading
import time
from abc import ABC, abstractmethod
from enum import Enum

# from .. import get_current_time
from ..heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
from ZeroMQFramework.common.node_type import ZeroMQNodeType
from ..common.socket_monitor import ZeroMQSocketMonitor
import zmq
from loguru import logger


class ZeroMQHeartbeatType(Enum):
    SENDER = "sender"
    RECEIVER = "receiver"


class ZeroMQHeartbeat(ABC):
    def __init__(self, context: zmq.Context, node_id: str, session_id: str, node_type: ZeroMQNodeType,
                 config: ZeroMQHeartbeatConfig):
        self.context = context
        self.node_id = node_id
        self.session_id = session_id
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

    def setup_socket_monitor(self):
        pass

    def is_connected(self):
        return self.socket_monitor.is_connected()

    def create_node_info(self, node_id: str, session_id: str, node_type: str):
        return {
            'node_id': node_id,
            'session_id': session_id,
            'node_type': node_type,
            'last_heartbeat': get_current_time(),
            'missed_count': 0
        }

    def start(self):
        # Always use demon to avoid blocking the main app from exiting
        logger.debug("Starting heartbeat thread")
        self.heartbeat_thread = threading.Thread(target=self._run, daemon=True)
        self.heartbeat_thread.start()

    def connect(self, bind=False):
        while self.running:
            try:
                connection_string = self.config.connection.get_connection_string(bind)
                logger.debug(f'heartbeat connecting to {connection_string}')
                # Always start the monitor before connecting with the socket. This ensures that you capture the
                # initial events I use monitor on sender only as the senders will send the heartbeat and will know if
                # the remote node is up or down
                if self.get_heartbeat_type() is ZeroMQHeartbeatType.SENDER:
                    logger.debug(f'starting socket monitor')
                    self.socket_monitor.start()  # Start the monitor after connecting
                if bind:
                    self.socket.bind(connection_string)
                    logger.info(f'heartbeat receiver bound successfully. {connection_string}')
                else:
                    self.socket.connect(connection_string)
                    logger.info(f'heartbeat sender connected successfully. {connection_string}')
                break
            except zmq.ZMQError as e:
                logger.error(f"ZMQ Error occurred during connect: ", e)
                time.sleep(self.config.interval)
                self._reinitialize_socket()
            except Exception as e:
                logger.error(f"unknown exception occurred during connect: ", e)
                time.sleep(self.config.interval)
                self._reinitialize_socket()

    def _reinitialize_socket(self):
        logger.info(f'reinitializing socket')
        if self.socket:
            self.socket.close()
        new_socket = self.context.socket(self.get_socket_type())
        self.socket = new_socket
        self.socket_monitor.reset_socket(new_socket)

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
