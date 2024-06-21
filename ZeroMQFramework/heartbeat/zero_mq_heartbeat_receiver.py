import threading
import time
from collections import defaultdict

import zmq

from ..heartbeat import ZeroMQHeartbeatConfig, ZeroMQHeartbeat
from ..helpers.zero_mq_node_type import ZeroMQNodeType
from ..helpers.zero_mq_event import ZeroMQEvent
from ..helpers.utils import *


class ZeroMQHeartbeatReceiver(ZeroMQHeartbeat):
    def __init__(self, context: zmq.Context, node_id: str, node_type: ZeroMQNodeType, config: ZeroMQHeartbeatConfig):
        super().__init__(context, node_id, node_type, config)
        self.node_heartbeats = defaultdict(lambda: (0, 0))
        self.lock = threading.Lock()

    def get_socket_type(self):
        return zmq.ROUTER

    def handle_heartbeat(self, node_id: str):
        with self.lock:
            self.node_heartbeats[node_id] = (get_current_time(), 0)
        # print(f"Custom handler: Heartbeat received from {node_id}")

    def check_missed_heartbeats(self):
        current_time = get_current_time()
        nodes_to_remove = []
        with self.lock:
            for node_id, (last_heartbeat, missed_count) in list(self.node_heartbeats.items()):
                if current_time - last_heartbeat > (self.config.timeout * 1000):
                    missed_count += 1
                    if missed_count > self.config.max_missed:
                        nodes_to_remove.append(node_id)
                        print(f"Node {node_id} removed after missing {missed_count} heartbeats")
                    else:
                        self.node_heartbeats[node_id] = (last_heartbeat, missed_count)

            for node_id in nodes_to_remove:
                del self.node_heartbeats[node_id]

    def poll_sockets(self, poller):
        socks = dict(poller.poll(self.config.interval * 1000))
        if self.socket in socks and socks[self.socket] == zmq.POLLIN:
            message = self.socket.recv_multipart()
            parsed_message = parse_message(message)
            if parsed_message["event_name"] == ZeroMQEvent.HEARTBEAT.value:
                node_id = parsed_message["event_data"]["node_id"]
                self.handle_heartbeat(node_id)

    def _run(self):
        self.connect(bind=True)
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        while self.running:
            try:
                self.poll_sockets(poller)
                self.check_missed_heartbeats()
            except zmq.ZMQError as e:
                print(f"ZMQ Error occurred: {e}")
                self.connect()
            except Exception as e:
                print(f"Unknown exception occurred: {e}")
                self.connect()
