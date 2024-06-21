import threading
import time
from abc import ABC, abstractmethod
from ..heartbeat.zero_mq_heartbeat_config import ZeroMQHeartbeatConfig
from ..helpers.zero_mq_node_type import ZeroMQNodeType

import zmq


class ZeroMQHeartbeat(ABC):
    def __init__(self, context: zmq.Context, node_id: str, node_type: ZeroMQNodeType, config: ZeroMQHeartbeatConfig):
        self.context = context
        self.node_id = node_id
        self.node_type = node_type
        self.config = config
        self.running = True
        self.socket = self.context.socket(config.socket_type)

    def start(self):
        # Always use demon to avoid blocking the main app from exiting
        threading.Thread(target=self._run, daemon=True).start()

    def connect(self, bind=False):
        while self.running:
            try:
                connection_string = self.config.connection.get_connection_string(bind)
                if bind:
                    self.socket.bind(connection_string)
                else:
                    self.socket.connect(connection_string)
                break
            except zmq.ZMQError as e:
                print(f"ZMQ Error occurred during connect: {e}")
                time.sleep(self.config.interval)
                self.socket.close()
                self.socket = self.context.socket(self.config.socket_type)
            except Exception as e:
                print(f"Unknown exception occurred during connect: {e}")
                time.sleep(self.config.interval)
                self.socket.close()
                self.socket = self.context.socket(self.config.socket_type)

    def stop(self):
        self.running = False
        self.socket.close()

    @abstractmethod
    def _run(self):
        pass
