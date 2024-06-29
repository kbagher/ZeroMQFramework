from abc import abstractmethod

import zmq
import threading
import signal
from typing import Callable, Any, Optional
from loguru import logger
from ZeroMQFramework.common.connection_protocol import ZeroMQConnection
from ZeroMQFramework.common.node_type import ZeroMQNodeType
from ZeroMQFramework.heartbeat.heartbeat_sender import ZeroMQHeartbeatSender
from ZeroMQFramework.heartbeat.heartbeat_receiver import ZeroMQHeartbeatReceiver
from ZeroMQFramework.heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
from ZeroMQFramework.common.socket_monitor import ZeroMQSocketMonitor
from .socket_status import ZeroMQSocketStatus

from ..helpers.utils import *


class ZeroMQBase(threading.Thread):
    def __init__(self, config_file: str, connection: ZeroMQConnection, node_type: ZeroMQNodeType,
                 handle_message: Callable[[dict], Any] = None, context: Optional[zmq.Context] = None,
                 heartbeat_config: Optional[ZeroMQHeartbeatConfig] = None):
        threading.Thread.__init__(self)
        self.config_file = config_file

        # not used in router as it has front- and back-end connections which is maintained internally by the router
        self.connection = connection
        self.handle_message = handle_message
        self.shutdown_requested = False
        self.context = context or zmq.Context()
        self.node_type = node_type
        self._is_connected = threading.Event()
        self._is_connected_timeout = 2  # in seconds
        self.socket_status = ZeroMQSocketStatus.CLOSED

        self.node_id = self.load_or_generate_node_id()
        self.session_id = get_uuid_hex(16)
        self.socket = self.context.socket(self.get_socket_type())
        self.socket.setsockopt(zmq.IDENTITY, self.get_socket_identity())
        self.socket_monitor = ZeroMQSocketMonitor(self.context, self.socket,
                                                  on_socket_closed_callback=self.socket_closed_callback,
                                                  on_socket_connect_callback=self.socket_connect_callback,
                                                  on_socket_disconnect_callback=self.socket_disconnect_callback)
        self.socket_monitor.start()

        self.heartbeat_config = heartbeat_config
        self.heartbeat_enabled = heartbeat_config is not None
        self.heartbeat = self.init_heartbeat()

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)
        self.daemon = True
        self.log_node_details()
        self._socket_requires_rest = False  # used if a socket is in ann invalid state and needs to be rest

    def get_socket_type(self):
        if self.node_type == ZeroMQNodeType.WORKER:
            return zmq.DEALER
        elif self.node_type == ZeroMQNodeType.SERVER:
            return zmq.REP
        elif self.node_type == ZeroMQNodeType.CLIENT:
            return zmq.REQ
        elif self.node_type == ZeroMQNodeType.ROUTER:
            return zmq.ROUTER
        else:
            raise ValueError(f"Unknown node type: {self.node_type}")

    def get_socket_identity(self):
        """
        Retrieve the socket identity by combining the node ID and session ID.
        Be careful here with the id, if multiple clients have the same ID and
        the router or server will get confused and will cause connections issues.
        So it's better to combine the node id with its random session id

        :return: The socket identity, encoded in UTF-8.
        :rtype: bytes
        """
        return f"{self.node_id}_{self.session_id}".encode('utf-8')

    def _reinitialize_socket(self):
        logger.debug("Reinitializing socket...")
        self._is_connected.clear()
        if self.socket is not None:
            logger.debug("Closing current socket")
            self.socket.close()
        self.socket = self.context.socket(self.get_socket_type())
        logger.debug("New socket created")
        self.socket.setsockopt(zmq.IDENTITY, self.get_socket_identity())
        logger.debug(f"New socket created st identity {self.get_socket_identity()}")
        self.socket_monitor.reset_socket(self.socket)
        self.socket_requires_reset = False

    def log_node_details(self):
        connection_string = self.connection.get_connection_string(bind=False)
        logger.info(
            f"Node Details ==> Node ID: {self.node_id}, Session ID: {self.session_id}, Node Type: {self.node_type} "
            f"Config File: {self.config_file}, Connection String: {connection_string}, "
            f"Heartbeat Enabled: {self.heartbeat_enabled}, Heartbeat Interval: {self.heartbeat_config.interval if self.heartbeat_enabled else 'N/A'}, "
            f"Heartbeat Timeout: {self.heartbeat_config.timeout if self.heartbeat_enabled else 'N/A'}, "
            f"Heartbeat Max Missed: {self.heartbeat_config.max_missed if self.heartbeat_enabled else 'N/A'}")

    def init_heartbeat(self):
        if self.heartbeat_enabled:
            # workers and client always send heartbeat
            if self.node_type in {ZeroMQNodeType.WORKER, ZeroMQNodeType.CLIENT}:
                return ZeroMQHeartbeatSender(context=self.context, node_id=self.node_id, session_id=self.session_id,
                                             node_type=self.node_type, config=self.heartbeat_config)
            # Routers and servers always receive heartbeats
            elif self.node_type in {ZeroMQNodeType.SERVER, ZeroMQNodeType.ROUTER}:
                return ZeroMQHeartbeatReceiver(context=self.context, node_id=self.node_id, session_id=self.session_id,
                                               node_type=self.node_type, config=self.heartbeat_config)
        return None

    def is_connected(self):
        return self._is_connected.is_set()

    @property
    def socket_requires_reset(self):
        return self._socket_requires_rest

    @socket_requires_reset.setter
    def socket_requires_reset(self, reset: bool = True):
        self._socket_requires_rest = reset

    def socket_connect_callback(self):
        """
        Callback method for socket monitor.
        This method is called when a socket connection is established.

        :return: None
        """
        self._is_connected.set()
        self.socket_status = ZeroMQSocketStatus.CONNECTED
        logger.debug(f"{self.node_type.value}: socket connection established")

    def socket_disconnect_callback(self):
        """
        Callback method for socket monitor.
        This method is called when a socket disconnected.

        :return: None

        """
        self._is_connected.clear()
        self.socket_status = ZeroMQSocketStatus.DISCONNECTED
        logger.debug(f"{self.node_type.value}: socket disconnected")

    def socket_closed_callback(self):
        """
        Callback method called when the socket is closed.
        This method is called when a socket closed.

        :return: None
        """
        self._is_connected.clear()
        self.socket_status = ZeroMQSocketStatus.CLOSED
        logger.debug(f"{self.node_type.value}: socket closed")

    def wait_for_connection(self, timeout: float = None):
        """
        Wait for a connection to be established.

        :param timeout: Maximum time to wait for the connection in seconds.
                        If not specified, the default timeout value is used.
        :type timeout: float
        :return: Returns True if a connection was established within the specified timeout,
                 otherwise returns False.
        :rtype: bool
        """
        if timeout is None:
            timeout = self._is_connected_timeout

        if not self._is_connected.wait(timeout=timeout):
            logger.debug(f"Wait for connection timed out for node {self.node_id} after "
                         f"{self._is_connected_timeout} seconds")
            return False
        return True

    def load_or_generate_node_id(self):
        """
        Loads the node_id from the configuration file if it exists,
        otherwise generates a new node_id and saves it in the configuration file.

        :return: The loaded or generated node_id
        :rtype: str
        """
        try:
            config = load_config(self.config_file, self.node_type.value.lower())
            node_id = config.get('node_id')
            if not node_id:
                raise ValueError("node_id is empty in the configuration file.")
        except ValueError:
            logger.warning("node_id is empty in the configuration file.")
            node_id = get_uuid_hex()
            save_config(self.config_file, self.node_type.value.lower(), 'node_id', node_id)
            logger.warning(f"New node id ({node_id}) is generated and saved in the config under node_id")
        return node_id

    def request_shutdown(self, signum, frame):
        logger.warning(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_requested = True

    def cleanup(self):
        """
        Perform cleanup by stopping the socket monitor, stopping the heartbeat, closing the socket,
        terminating the context, and logging the cleanup completion.
        If you are going to add more connections, working threads, etc...
        make sure the cleanup the resources in order.
        For example, you can't terminate the context before closing the socket, duh!!

        :return: None
        """
        logger.info("Performing cleanup...")
        if self.socket_monitor:
            self.socket_monitor.stop()
        if self.heartbeat:
            logger.info(f"{self.node_type.value} is calling stop heartbeat...")
            self.heartbeat.stop()  # Ensure heartbeat thread is stopped
        if self.socket:
            logger.info(f"{self.node_type.value} is closing socket...")
            self.socket.close()  # Close the socket
        logger.info(f"{self.node_type.value} is terminating context...")
        self.context.term()  # Terminate the context
        logger.info("Cleanup complete.")
