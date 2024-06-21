import time
import zmq
from ..heartbeat import ZeroMQHeartbeatConfig, ZeroMQHeartbeat
from ..helpers.zero_mq_node_type import ZeroMQNodeType
from ..helpers.utils import create_message
from ..helpers.zero_mq_event import ZeroMQEvent
from ..common.zero_mq_socket_monitor import ZeroMQSocketMonitor


class ZeroMQHeartbeatSender(ZeroMQHeartbeat):
    def __init__(self, context: zmq.Context, node_id: str, node_type: ZeroMQNodeType, config: ZeroMQHeartbeatConfig):
        super().__init__(context, node_id, node_type, config)
        self.socket_monitor = ZeroMQSocketMonitor(context, self.socket)

    def get_socket_type(self):
        return zmq.DEALER

    def _run(self):
        self.connect()
        if self.socket_monitor.is_connected():
            print("Heartbeat sender connected to endpoint node. Sending...")
        while self.running:
            try:
                time.sleep(self.config.interval)
                if not self.socket_monitor.is_connected():
                    print("Cannot reach router, discarding heartbeat...")
                    continue

                message = create_message(ZeroMQEvent.HEARTBEAT.value, {"node_id": self.node_id})
                print(message)
                self.socket.send_multipart(message)
                # print(f"Heartbeat sent: {message}")
            except zmq.ZMQError as e:
                print(f"ZMQ Error occurred: {e}")
                self.connect()
            except Exception as e:
                print(f"Unknown exception occurred: {e}")
                self.connect()
