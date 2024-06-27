import zmq
import threading
import signal
from typing import Callable, Any, Optional
from loguru import logger
from ..common.processing_base import ZeroMQProcessingBase
from ZeroMQFramework.common.connection_protocol import ZeroMQConnection
from ZeroMQFramework.common.node_type import ZeroMQNodeType
from ZeroMQFramework.heartbeat.heartbeat_sender import ZeroMQHeartbeatSender
from ZeroMQFramework.heartbeat.heartbeat_receiver import ZeroMQHeartbeatReceiver
from ZeroMQFramework.heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
from ..helpers.utils import get_uuid_hex, create_message, parse_message


class ZeroMQBase(threading.Thread, ZeroMQProcessingBase):
    def __init__(self, connection: ZeroMQConnection, node_type: ZeroMQNodeType,
                 handle_message: Callable[[dict], Any] = None, context: Optional[zmq.Context] = None,
                 heartbeat_config: Optional[ZeroMQHeartbeatConfig] = None):
        threading.Thread.__init__(self)
        self.connection = connection
        self.handle_message = handle_message
        self.shutdown_requested = False
        self.context = context or zmq.Context()
        self.node_type = node_type

        # Initialize the socket type based on node type
        self.socket = self.context.socket(self.get_socket_type())
        self.node_id = get_uuid_hex()
        self.socket.setsockopt(zmq.IDENTITY, self.node_id.encode('utf-8'))

        self.heartbeat_config = heartbeat_config
        self.heartbeat_enabled = heartbeat_config is not None
        self.heartbeat = self.init_heartbeat()

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)
        self.daemon = True

    def get_socket_type(self):
        if self.node_type == ZeroMQNodeType.WORKER:
            return zmq.DEALER
        elif self.node_type == ZeroMQNodeType.SERVER:
            return zmq.REP
        elif self.node_type == ZeroMQNodeType.CLIENT:
            return zmq.REQ
        elif self.node_type == ZeroMQNodeType.ROUTER:
            return zmq.ROUTER
        elif self.node_type == ZeroMQNodeType.DEALER:
            return zmq.DEALER
        else:
            raise ValueError(f"Unknown node type: {self.node_type}")

    def init_heartbeat(self):
        if self.heartbeat_enabled:
            if self.node_type in {ZeroMQNodeType.WORKER, ZeroMQNodeType.CLIENT}:
                return ZeroMQHeartbeatSender(context=self.context, node_id=self.node_id, node_type=self.node_type,
                                             config=self.heartbeat_config)
            elif self.node_type in {ZeroMQNodeType.SERVER, ZeroMQNodeType.ROUTER}:
                return ZeroMQHeartbeatReceiver(context=self.context, node_id=self.node_id, node_type=self.node_type,
                                               config=self.heartbeat_config)
        return None

    def request_shutdown(self, signum, frame):
        logger.warning(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_requested = True

    def cleanup(self):
        logger.info("Performing cleanup...")
        if self.heartbeat:
            self.heartbeat.stop()
        self.socket.close()
        self.context.term()

    def handle_message(self, message: dict) -> Any:
        raise NotImplementedError("Subclasses must implement this method.")
