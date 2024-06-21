import threading
import time
from abc import ABC, abstractmethod
from ..heartbeat.heartbeat_config import ZeroMQHeartbeatConfig
from ..helpers.node_type import ZeroMQNodeType
from ..common.socket_monitor import ZeroMQSocketMonitor
import zmq


class ZeroMQHeartbeat(ABC):
    def __init__(self, context: zmq.Context, node_id: str, node_type: ZeroMQNodeType, config: ZeroMQHeartbeatConfig):
        self.context = context
        self.node_id = node_id
        self.node_type = node_type
        self.config = config
        self.running = True
        self.socket = self.context.socket(self.get_socket_type())
        self.socket_monitor = ZeroMQSocketMonitor(context, self.socket)

    def get_socket_type(self):
        raise NotImplementedError("Subclasses must implement get_socket_type method")

    def start(self):
        # Always use demon to avoid blocking the main app from exiting
        threading.Thread(target=self._run, daemon=True).start()

    def connect(self, bind=False):
        while self.running:
            try:
                connection_string = self.config.connection.get_connection_string(bind)
                if bind:
                    self.socket.bind(connection_string)
                    print(f'Bind successfully connected. {connection_string}')
                else:
                    self.socket.connect(connection_string)
                    print(f'Successfully connected. {connection_string}')
                if self.node_type == ZeroMQNodeType.WORKER:
                    self.socket_monitor.start()  # Start the monitor after connecting
                break
            except zmq.ZMQError as e:
                print(f"ZMQ Error occurred during connect: {e}")
                time.sleep(self.config.interval)
                self.socket.close()
                self.socket = self.context.socket(self.get_socket_type())
            except Exception as e:
                print(f"Unknown exception occurred during connect: {e}")
                time.sleep(self.config.interval)
                self.socket.close()
                self.socket = self.context.socket(self.get_socket_type())

    def stop(self):
        self.running = False
        if self.socket_monitor.monitor_socket:
            self.socket_monitor.monitor_socket.close()
        self.socket.close()

    @abstractmethod
    def _run(self):
        raise NotImplementedError("Subclasses must implement _run method")
