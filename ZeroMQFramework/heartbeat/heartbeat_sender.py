import time
import zmq
from ..heartbeat.heartbeat import ZeroMQHeartbeat, ZeroMQHeartbeatType
from ..heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
from ZeroMQFramework.common.node_type import ZeroMQNodeType
from ..helpers.utils import create_message
from ZeroMQFramework.common.event import ZeroMQEvent
from loguru import logger


class ZeroMQHeartbeatSender(ZeroMQHeartbeat):
    def __init__(self, context: zmq.Context, node_id: str, session_id: str, node_type: ZeroMQNodeType,
                 config: ZeroMQHeartbeatConfig):
        super().__init__(context, node_id, session_id, node_type, config)

    def get_socket_type(self):
        return zmq.DEALER

    def get_heartbeat_type(self):
        return ZeroMQHeartbeatType.SENDER

    def _run(self):
        logger.info("Heartbeat sender: _run")
        self.connect()

        if self.is_connected():
            logger.info("Heartbeat sender: connected to endpoint node. Sending...")
        while self.running:
            try:
                time.sleep(self.config.interval)
                if not self.running:
                    break
                if not self.is_connected():
                    logger.warning("Heartbeat sender: Heartbeat cannot reach node, discarding heartbeat...")
                    continue

                message = create_message(ZeroMQEvent.HEARTBEAT.value,
                                         {"node_id": self.node_id, "session_id": self.session_id},
                                         include_empty_frame=True)
                # print(message)
                self.socket.send_multipart(message)
                # print(f"Heartbeat sent: {message}")
            except zmq.ZMQError as e:
                logger.error(f"Heartbeat sender: ZMQ Error occurred: {e}")
                self.connect()
            except Exception as e:
                logger.error(f"Heartbeat sender: Unknown exception occurred: {e}")
                self.connect()
        print("not in while")
