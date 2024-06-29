import zmq

from ZeroMQFramework.common.connection_protocol import *
from ZeroMQFramework.common.socket_status import ZeroMQSocketStatus
from ZeroMQFramework.helpers.utils import *
from ZeroMQFramework.helpers.error import *
from ZeroMQFramework.common.base import ZeroMQBase
from ZeroMQFramework.heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
from ZeroMQFramework.common.node_type import ZeroMQNodeType


class ZeroMQClient(ZeroMQBase):
    def __init__(self, config_file: str, connection: ZeroMQConnection,
                 heartbeat_config: ZeroMQHeartbeatConfig = None, timeout: int = 5):
        super().__init__(config_file, connection, ZeroMQNodeType.CLIENT, None, None, heartbeat_config)
        self.timeout = timeout * 1000  # convert to ms. Don't't change the multiplication unless u know what you are
        # doing!

        self.heartbeat_started = False
        self.poller = zmq.Poller()
        self.connection_string = self.connection.get_connection_string(bind=False)
        self._configure_socket()
        self._reinitialize = False

    def _configure_socket(self):
        """Configure the ZMQ socket with the appropriate options."""
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)  # milliseconds
        self.socket.setsockopt(zmq.SNDTIMEO, self.timeout)  # milliseconds

    def connect(self):
        """
        Establishes a connection using the generated connection string.

        :return: True if the connection is successful, False otherwise.
        """
        logger.debug(f"Connect")
        logger.debug(f"heartbeat_enabled = {self.heartbeat_enabled}, heartbeat_started = {self.heartbeat_started}")
        if self.heartbeat_enabled and not self.heartbeat_started:
            logger.info('Client: Starting heartbeat')
            self.heartbeat.start()
            self.heartbeat_started = True

        logger.debug(f"socket_status = {self.socket_status.value}, _reinitialize = {self._reinitialize}, "
                     f"socket_requires_reset = {self.socket_requires_reset}")
        if self.socket_status == ZeroMQSocketStatus.CLOSED and self._reinitialize or self.socket_requires_reset:
            logger.info("Client: Reinitializing socket due to closed status")
            self._reinitialize_socket()
            self._configure_socket()

        logger.info(f'Client: establishing connection on {self.connection_string}...')
        self.socket.connect(self.connection_string)

        if self.wait_for_connection():
            logger.info(f'Client: connected on {self.connection_string} successfully')
            self._reinitialize = True
            return True
        else:
            logger.warning(f'Client: failed to connect on {self.connection_string}')
            self._reinitialize = False
            return False

    def send_message(self, event_name: str, event_data: dict):
        """
        Sends a message using the ZeroMQ socket.

        :param event_name: The name of the event being sent.
        :param event_data: The data associated with the event.
        :return: The response received after sending the message.
        :raises ZeroMQQSocketDisconnected: If the socket state is disconnected.
        :raises ZeroMQQSocketClosed: If the socket state is closed.
        :raises ZeroMQTimeoutError: If no response is received within the timeout period.
        :raises ZeroMQQSocketInvalid: If the socket is in an invalid state.
        :raises ZeroMQClientError: If a general ZMQError occurs.
        """
        if self.socket_status == ZeroMQSocketStatus.DISCONNECTED:
            raise ZeroMQQSocketDisconnected("Socket state is disconnected")
        elif self.socket_status == ZeroMQSocketStatus.CLOSED:
            raise ZeroMQQSocketClosed("Socket state is closed")

        message = create_message(event_name, event_data)

        try:
            if self.socket_status == ZeroMQSocketStatus.CLOSED:
                raise zmq.ZMQError
            self.socket.send_multipart(message)
            return self.receive_message()
        except zmq.Again:
            err = f"Client: No response received within the timeout period {self.timeout / 1000} seconds"
            logger.warning(err)
            raise ZeroMQTimeoutError(err)
        except zmq.ZMQError as e:
            if e.errno in (zmq.EFSM, zmq.EAGAIN):
                err = f"Socket is in an invalid state. {e}"
                logger.error(err)
                self.socket_requires_reset = True
                raise ZeroMQQSocketInvalid(err)
            else:
                err = f"Client: ZMQError occurred: {e}."
                logger.error(err)
                raise ZeroMQClientError(err)

    def receive_message(self):
        reply = self.socket.recv_multipart()
        return parse_message(reply)

    def cleanup(self):
        logger.info("Client: Cleaning up client...")
        super().cleanup()
        logger.info("Client: Cleaned up ZeroMQ sockets and context.")
