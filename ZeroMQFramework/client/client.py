import zmq

from ZeroMQFramework.common.connection_protocol import *
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
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)
        self.socket.setsockopt(zmq.SNDTIMEO, self.timeout)
        self.connected = False
        self.heartbeat_started = False
        self.poller = zmq.Poller()

    def connect(self):
        connection_string = self.connection.get_connection_string(bind=False)
        self.socket.connect(connection_string)
        self.connected = True
        if self.heartbeat_enabled and not self.heartbeat_started:
            logger.info('Client: Starting heartbeat')
            self.heartbeat.start()
            self.heartbeat_started = True

        logger.info(f'Client: Client connected to node at {connection_string}')

    def reconnect(self):
        self._reinitialize_socket()
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)
        self.socket.setsockopt(zmq.SNDTIMEO, self.timeout)
        attempts = 0
        while attempts < self.retry_attempts:
            try:
                self.connect()
                return
            except zmq.ZMQError as e:
                logger.warning(f"Client: Reconnect attempt {attempts + 1}/{self.retry_attempts} failed: {e}")
                attempts += 1
                time.sleep(self.retry_timeout / 1000)
        raise ZeroMQConnectionError("Client: Unable to reconnect after several attempts")

    def send_message(self, event_name: str, event_data: dict):
        if not self.connected:
            self.reconnect()

        message = create_message(event_name, event_data)
        attempts = 0
        while attempts < self.retry_attempts and not self.shutdown_requested:
            try:
                self.socket.send_multipart(message)
                res = self.receive_message()
                return res
            except zmq.Again:
                logger.warning("Client: No response received within the timeout period, retrying...")
                attempts += 1
                time.sleep(self.retry_timeout / 1000)
            except zmq.ZMQError as e:
                if e.errno in (zmq.EFSM, zmq.EAGAIN):
                    logger.error(f"Client: Socket is in an invalid state, reconnecting socket. {e}")
                    self.connected = False
                    self.reconnect()
                else:
                    logger.error(f"Client: ZMQError occurred: {e}, reconnecting socket.")
                    self.connected = False
                    self.reconnect()
        raise ZeroMQTimeoutError("Client: Unable to send message after several attempts")

    def receive_message(self):
        reply = self.socket.recv_multipart()
        return parse_message(reply)

    def socket_connect_callback(self):
        pass

    def socket_closed_callback(self):
        pass

    def socket_disconnect_callback(self):
        pass

    def cleanup(self):
        logger.info("Client: Cleaning up client...")
        super().cleanup()
        logger.info("Client: Cleaned up ZeroMQ sockets and context.")
