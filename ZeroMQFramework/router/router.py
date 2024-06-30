from ZeroMQFramework import ZeroMQBase
from ZeroMQFramework.common.connection_protocol import *
from ZeroMQFramework.router.routing_proxy import ZeroMQRoutingProxy
from ZeroMQFramework.router.routing_strategy import ZeroMQRoutingStrategy
from loguru import logger
from ZeroMQFramework.common.node_type import ZeroMQNodeType
from ZeroMQFramework.heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
import zmq


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
        self.strategy = strategy if strategy else ZeroMQRoutingProxy()
        self.configure_socket()

    def configure_socket(self):
        self.frontend_socket = self.context.socket(zmq.ROUTER)
        self.frontend_socket.setsockopt(zmq.IDENTITY, self.get_socket_identity())

        self.backend_socket = self.context.socket(zmq.DEALER)
        self.backend_socket.setsockopt(zmq.IDENTITY, self.get_socket_identity())

        self.frontend_connection_string = self.frontend_connection.get_connection_string(bind=True)
        self.backend_connection_string = self.backend_connection.get_connection_string(bind=True)

        self.poller.register(self.frontend_socket, zmq.POLLIN)
        self.poller.register(self.backend_socket, zmq.POLLIN)

    def start(self):

        try:
            self.frontend_socket.bind(self.frontend_connection_string)
            self.backend_socket.bind(self.backend_connection_string)

            if self.heartbeat_enabled:
                self.heartbeat.start()

            logger.info(f"router started and bound to frontend {self.frontend_connection_string} "
                        f"and backend {self.backend_connection_string}")

            self.strategy.route(self.frontend_socket, self.backend_socket,
                                poller=self.poller, poll_timeout=self.poller_timeout)

        except zmq.ZMQError as e:
            logger.error(f"ZMQ Error occurred: {e}")
        except Exception as e:
            logger.error(f"Unknown exception occurred: {e}")
        finally:
            logger.info("Router is stopping...")
            logger.info("Cleaning up...")
            self.cleanup()


    def cleanup(self):
        logger.info("Router is shutting down, performing cleanup...")
        if self.frontend_socket:
            self.frontend_socket.close()
        if self.backend_socket:
            self.backend_socket.close()
        super().cleanup()
        logger.info("Cleaned up ZeroMQ sockets and context.")
