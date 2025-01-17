import threading
from typing import Dict

import zmq

from ..heartbeat.node_info import ZeroMQNodeInfo
from ..heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
from ..heartbeat.heartbeat import ZeroMQHeartbeat, ZeroMQHeartbeatType
from ZeroMQFramework.common.node_type import ZeroMQNodeType
from ZeroMQFramework.common.event import ZeroMQEvent
from ..helpers.utils import *


class ZeroMQHeartbeatReceiver(ZeroMQHeartbeat):
    def __init__(self, context: zmq.Context, node_id: str, session_id: str, node_type: ZeroMQNodeType,
                 config: ZeroMQHeartbeatConfig):
        super().__init__(context, node_id, session_id, node_type, config)
        self.node_heartbeats: Dict[str, ZeroMQNodeInfo] = {}
        self.lock = threading.Lock()
        self.connected_nodes = set()

    def get_socket_type(self):
        return zmq.ROUTER

    def get_heartbeat_type(self):
        return ZeroMQHeartbeatType.RECEIVER

    def get_node_key(self, node_id: str, session_id: str):
        return f"{node_id}_{session_id}"

    def handle_heartbeat(self, node_info: ZeroMQNodeInfo):
        with (self.lock):
            node_key = self.get_node_key(node_info.node_id, node_info.session_id)
            if node_key not in self.connected_nodes:
                self.connected_nodes.add(node_key)
                logger.info(f"node connected: {self.get_details_string(node_info)}")
            node_info.last_heartbeat = get_current_time()
            node_info.missed_count = 0
            self.node_heartbeats[node_key] = node_info
            self.log_connected_nodes()

    def log_connected_nodes(self):
        if self.connected_nodes:
            logger.debug("Connected nodes:")
            for node_key in self.connected_nodes:
                node_info = self.node_heartbeats[node_key]
                logger.debug(
                    f"Node ID: {node_info.node_id}, Session ID: {node_info.session_id},"
                    f"Type: {node_info.node_type.value}")

    def get_details_string(self, node_info: ZeroMQNodeInfo) -> str:
        return (f"node_type = {node_info.node_type.value}, node_id: {node_info.node_id}, "
                f"session_id = {node_info.session_id}")

    def check_missed_heartbeats(self):
        current_time = get_current_time()
        nodes_to_remove = []
        with self.lock:
            for node_key, node_info in list(self.node_heartbeats.items()):
                if current_time - node_info.last_heartbeat > (self.config.timeout * 1000):
                    node_info.missed_count += 1
                    if node_info.missed_count > self.config.max_missed:
                        nodes_to_remove.append(node_key)
                        logger.info(
                            f"node disconnected: {self.get_details_string(node_info)}, "
                            f"missing {node_info.missed_count} heartbeats")
                        self.log_connected_nodes()
                    else:
                        self.node_heartbeats[node_key] = node_info

            for node_key in nodes_to_remove:
                del self.node_heartbeats[node_key]
                self.connected_nodes.discard(node_key)

    def poll_sockets(self, poller):
        socks = dict(poller.poll(self.config.interval * 1000))
        if self.socket in socks and socks[self.socket] == zmq.POLLIN:
            message = self.socket.recv_multipart()
            parsed_message = parse_message(message)
            if parsed_message["event_name"] == ZeroMQEvent.HEARTBEAT.value:
                node_info_dict = parsed_message["event_data"]
                node_info = ZeroMQNodeInfo.from_dict(node_info_dict)
                self.handle_heartbeat(node_info)

    def _run(self):
        self.connect(bind=True)
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        while self.running:
            try:
                self.poll_sockets(poller)
                self.check_missed_heartbeats()
            except zmq.ZMQError as e:
                logger.error(f"ZMQ Error occurred: {e}")
                self.connect()
            except Exception as e:
                logger.error(f"Unknown exception occurred: {e}")
                self.connect()
