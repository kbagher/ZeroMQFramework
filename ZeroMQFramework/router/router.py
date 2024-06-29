from ZeroMQFramework import ZeroMQBase
from ZeroMQFramework.common.connection_protocol import *
from ZeroMQFramework.helpers.utils import *
from ZeroMQFramework.router.routing_strategy import *
from ZeroMQFramework.helpers.error import *
from loguru import logger
from ZeroMQFramework.common.node_type import ZeroMQNodeType
from ZeroMQFramework.heartbeat.heartbeat_receiver import ZeroMQHeartbeatReceiver
from ZeroMQFramework.heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
import zmq
import threading
import signal


class ZeroMQRouter(ZeroMQBase):
    def __init__(self, config_file: str, frontend_connection: ZeroMQConnection, backend_connection: ZeroMQConnection,
                 heartbeat_config: ZeroMQHeartbeatConfig = None,
                 strategy: Optional[ZeroMQRoutingStrategy] = None):
        super().__init__(config_file, connection=frontend_connection, node_type=ZeroMQNodeType.ROUTER,
                         handle_message=None, context=None, heartbeat_config=heartbeat_config)

        self.frontend_connection = frontend_connection
        self.frontend_socket = None
        self.frontend_connection_string = None
        self.backend_connection = backend_connection
        self.backend_socket = None
        self.backend_connection_string = None
        self.configure_socket()

        self.proxy_thread = None
        self.strategy = strategy

    def configure_socket(self):
        self.frontend_socket = self.context.socket(zmq.ROUTER)
        self.frontend_socket.setsockopt(zmq.IDENTITY, self.get_socket_identity())

        self.backend_socket = self.context.socket(zmq.DEALER)
        self.backend_socket.setsockopt(zmq.IDENTITY, self.get_socket_identity())

        self.frontend_connection_string = self.frontend_connection.get_connection_string(bind=True)
        self.backend_connection_string = self.backend_connection.get_connection_string(bind=True)

    def start(self):

        try:
            self.frontend_socket.bind(self.frontend_connection_string)
            self.backend_socket.bind(self.backend_connection_string)

            logger.info(f"router started and bound to frontend {self.frontend_connection_string} "
                        f"and backend {self.backend_connection_string}")

            if self.strategy:
                raise ValueError("This is not implemented yet")
            else:
                self._start_standard_proxy()

        except zmq.ZMQError as e:
            logger.error(f"ZMQ Error occurred: {e}")
        except Exception as e:
            logger.error(f"Unknown exception occurred: {e}")
        finally:
            logger.info("Router is stopping...")
            logger.info("Cleaning up...")
            self.cleanup()

    def _start_standard_proxy(self):
        try:
            while not self.shutdown_requested:
                zmq.proxy(self.frontend_socket, self.backend_socket)
        except zmq.ContextTerminated:
            if self.shutdown_requested:
                logger.info("Proxy shutdown gracefully")
            else:
                logger.error("ZMQ Proxy error: Context terminated unexpectedly")
        except zmq.ZMQError as e:
            logger.error(f"ZMQ Proxy error: {e}")

    def _start_custom_routing(self):
        pass
        # self.poller.register(self.frontend_socket, zmq.POLLIN)
        # self.poller.register(self.backend_socket, zmq.POLLIN)
        #
        # while not self.shutdown_requested:
        #     socks = dict(self.poller.poll(self.poller_timeout))
        #     if self.frontend_socket in socks and socks[self.frontend_socket] == zmq.POLLIN:
        #         message = self.frontend_socket.recv_multipart()
        #         self.backend_socket.send_multipart(message)
        #
        #     if self.backend_socket in socks and socks[self.backend_socket] == zmq.POLLIN:
        #         message = self.backend_socket.recv_multipart()
        #         self.strategy.route(self.backend_socket, self.backend_socket)

    def cleanup(self):
        logger.info("Router is shutting down, performing cleanup...")
        if self.frontend_socket:
            self.frontend_socket.close()
        if self.backend_socket:
            self.backend_socket.close()
        super().cleanup()
        logger.info("Cleaned up ZeroMQ sockets and context.")
