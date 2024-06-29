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
                 heartbeat_config: ZeroMQHeartbeatConfig = None, timeout: int = 5000,
                 retry_attempts: int = 3, retry_timeout: int = 1000):
        super().__init__(config_file, connection, ZeroMQNodeType.CLIENT, None, None, heartbeat_config)
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_timeout = retry_timeout
        self.heartbeat_started = False
        self.poller = zmq.Poller()
        self.connection_string = self.connection.get_connection_string(bind=False)
        self.configure_socket()
        self._reinitialize = False

    def configure_socket(self):
        """Configure the ZMQ socket with the appropriate options."""
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)  # milliseconds
        self.socket.setsockopt(zmq.SNDTIMEO, self.timeout)  # milliseconds

    def connect(self):
        """Establish the connection to the server."""
        if self.heartbeat_enabled and not self.heartbeat_started:
            logger.info('Client: Starting heartbeat')
            self.heartbeat.start()
            self.heartbeat_started = True

        if self.socket_status == ZeroMQSocketStatus.CLOSED and self._reinitialize:
            logger.info("Client: Reinitializing socket due to closed status")
            self._reinitialize_socket()
            self.configure_socket()

        logger.info(f'Client: establishing connection on {self.connection_string}...')
        self.socket.connect(self.connection_string)
        self._reinitialize = True

        if self.wait_for_connection():
            logger.info(f'Client: connected on {self.connection_string} successfully')
            return True
        else:
            logger.warning(f'Client: failed to connect on {self.connection_string}')
            return False

    def send_message(self, event_name: str, event_data: dict):
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
            logger.warning("Client: No response received within the timeout period")
            raise ZeroMQTimeoutError("No response received within the timeout period")
        except zmq.ZMQError as e:
            if e.errno in (zmq.EFSM, zmq.EAGAIN):
                logger.error(f"Client: Socket is in an invalid state. {e}")
                raise ZeroMQQSocketInvalid(f"Socket is in an invalid state. {e}")
            else:
                logger.error(f"Client: ZMQError occurred: {e}.")
                raise ZeroMQClientError(f"ZMQError occurred: {e}.")

    def receive_message(self):
        reply = self.socket.recv_multipart()
        return parse_message(reply)

    def cleanup(self):
        logger.info("Client: Cleaning up client...")
        super().cleanup()
        logger.info("Client: Cleaned up ZeroMQ sockets and context.")
