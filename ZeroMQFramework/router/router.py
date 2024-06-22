import uuid

from ZeroMQFramework.helpers.config import *
from ZeroMQFramework.helpers.utils import *
from ZeroMQFramework.router.routing_strategy import *
from ZeroMQFramework.helpers.error import *
from ZeroMQFramework.helpers.logger import logger
from ZeroMQFramework.helpers.node_type import ZeroMQNodeType
from ZeroMQFramework.heartbeat.heartbeat_receiver import ZeroMQHeartbeatReceiver
from ZeroMQFramework.heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
import zmq
import threading
import signal


class ZeroMQRouter:
    def __init__(self, frontend_connection: ZeroMQConnection, backend_connection: ZeroMQConnection, heartbeat_config: ZeroMQHeartbeatConfig = None,
                 strategy: Optional[ZeroMQRoutingStrategy] = None):
        self.frontend_connection = frontend_connection
        self.backend_connection = backend_connection
        self.shutdown_requested = True
        self.heartbeat_config = heartbeat_config
        self.frontend = None
        self.backend = None
        self.context = zmq.Context()
        self.strategy = strategy
        self.heartbeat_enabled = heartbeat_config is not None

        self.router_id = uuid.uuid4().__str__()

        if self.heartbeat_enabled:
            self.heartbeat_handler = ZeroMQHeartbeatReceiver(context=self.context, node_id=self.router_id,
                                                             node_type=ZeroMQNodeType.ROUTER,
                                                             config=self.heartbeat_config)
        else:
            self.heartbeat_handler = None

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, signum, frame):
        logger.warn(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_requested = False

    def start(self):
        context = self.context
        self.frontend = context.socket(zmq.ROUTER)
        self.frontend.setsockopt(zmq.IDENTITY, self.router_id.encode('utf-8'))  # Set the worker ID

        self.backend = context.socket(zmq.DEALER)
        self.backend.setsockopt(zmq.IDENTITY, self.router_id.encode('utf-8'))  # Set the worker ID

        frontend_connection_string = self.frontend_connection.get_connection_string(bind=True)
        backend_connection_string = self.backend_connection.get_connection_string(bind=True)

        try:
            self.frontend.bind(frontend_connection_string)
            self.backend.bind(backend_connection_string)

            logger.info(
                f"Router started and bound to frontend {frontend_connection_string} and backend {backend_connection_string}")

            proxy_thread = threading.Thread(target=self._start_proxy)
            proxy_thread.start()

            if self.heartbeat_enabled:
                self.heartbeat_handler.start()

            proxy_thread.join()

        except zmq.ZMQError as e:
            logger.error(f"ZMQ Error occurred: {e}")
        except Exception as e:
            logger.error(f"Unknown exception occurred: {e}")
        finally:
            logger.info("Router is stopping...")
            if self.heartbeat_enabled:
                logger.info("Heartbeat is stopping...")
                self.heartbeat_handler.stop()
            logger.info("Cleaning up...")
            self.cleanup()

    def _start_proxy(self):
        poller = zmq.Poller()
        poller.register(self.frontend, zmq.POLLIN)
        poller.register(self.backend, zmq.POLLIN)

        while self.shutdown_requested:
            socks = dict(poller.poll(timeout=4000))
            try:
                if self.frontend in socks and socks[self.frontend] == zmq.POLLIN:
                    message = self.frontend.recv_multipart()
                    self.backend.send_multipart(message)

                if self.backend in socks and socks[self.backend] == zmq.POLLIN:
                    message = self.backend.recv_multipart()
                    try:
                        parsed_message = parse_message(message)
                        if self.heartbeat_enabled and self.is_heartbeat(parsed_message):
                            self.heartbeat_handler.handle_heartbeat(parsed_message["event_data"]["worker_id"])
                    except ZeroMQMalformedMessage as e:
                        logger.error(f"Error in message parsing or handling: {e}")
                    else:
                        if self.strategy:
                            self.strategy.route(self.frontend, self.backend)
                        else:
                            self.frontend.send_multipart(message)
            except zmq.ZMQError as e:
                logger.error(f"ZMQ Error occurred: {e}")
            except Exception as e:
                logger.error(f"Unknown exception occurred: {e}")

        # Exited the loop (self.shutdown_requested is true)
        self.cleanup(poller)

    def is_heartbeat(self, parsed_message):
        try:
            return parsed_message["event_name"] == "HEARTBEAT"
        except ValueError:
            return False

    def cleanup(self, poller=None):
        logger.info("Router is shutting down, performing cleanup...")
        if self.heartbeat_enabled:
            logger.info("Heartbeat is stopping...")
            self.heartbeat_handler.stop()
        if self.frontend:
            self.frontend.close()
            if poller:
                poller.unregister(self.frontend)
        if self.backend:
            self.backend.close()
            if poller:
                poller.unregister(self.backend)
        self.context.term()
        logger.info("Cleaned up ZeroMQ sockets and context.")
