import threading
import time
from collections import defaultdict

import zmq

from ..heartbeat import ZeroMQHeartbeatConfig, ZeroMQHeartbeat
from ..helpers.zero_mq_node_type import ZeroMQNodeType


class ZeroMQHeartbeatReceiver(ZeroMQHeartbeat):
    def __init__(self, context: zmq.Context, node_id: str, node_type: ZeroMQNodeType, config: ZeroMQHeartbeatConfig):
        super().__init__(context, node_id, node_type, config)
        self.node_heartbeats = defaultdict(lambda: (0.0, 0.0))
        self.lock = threading.Lock()

    def handle_heartbeat(self, node_id: str):
        with self.lock:
            self.node_heartbeats[node_id] = (time.time(), 0)
        print(f"Custom handler: Heartbeat received from {node_id}")

    def _run(self):
        self.connect(bind=True)
        while self.running:
            try:
                current_time = time.time()
                with self.lock:
                    nodes_to_remove = []
                    for node_id, (last_heartbeat, missed_count) in list(self.node_heartbeats.items()):
                        if current_time - last_heartbeat > self.config.timeout:
                            missed_count += 1
                            if missed_count > self.config.max_missed:
                                nodes_to_remove.append(node_id)
                            else:
                                self.node_heartbeats[node_id] = (last_heartbeat, missed_count)

                    for node_id in nodes_to_remove:
                        del self.node_heartbeats[node_id]

                time.sleep(self.config.interval)
            except zmq.ZMQError as e:
                print(f"ZMQ Error occurred: {e}")
                self.connect()
            except Exception as e:
                print(f"Unknown exception occurred: {e}")
                self.connect()
