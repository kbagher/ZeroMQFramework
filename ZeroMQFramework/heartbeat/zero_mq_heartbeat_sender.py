import time
import zmq
from ..heartbeat import ZeroMQHeartbeatConfig, ZeroMQHeartbeat
from ..helpers.zero_mq_node_type import ZeroMQNodeType
from ..helpers.utils import create_message
from ..helpers.zero_mq_event import ZeroMQEvent


class ZeroMQHeartbeatSender(ZeroMQHeartbeat):
    def __init__(self, context: zmq.Context, node_id: str, node_type: ZeroMQNodeType, config: ZeroMQHeartbeatConfig):
        super().__init__(context, node_id, node_type, config)

    def _run(self):
        self.connect()
        while self.running:
            try:
                time.sleep(self.config.interval)
                message = create_message(ZeroMQEvent.HEARTBEAT.value, {"node_id": self.node_id})
                self.socket.send_multipart(message, zmq.NOBLOCK)
                print(f"Heartbeat sent: {message}")
            except zmq.ZMQError as e:
                print(f"ZMQ Error occurred: {e}")
                self.connect()
            except Exception as e:
                print(f"Unknown exception occurred: {e}")
                self.connect()
