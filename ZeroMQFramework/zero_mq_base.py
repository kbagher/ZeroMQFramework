import zmq
from concurrent.futures import ThreadPoolExecutor
from .config import ZeroMQProtocol
from typing import Any


class ZeroMQBase:
    def __init__(self, port: int, protocol: ZeroMQProtocol = ZeroMQProtocol.TCP, ipc_path: str = "/tmp/zmq.ipc"):
        self.protocol = protocol
        self.port = port
        self.ipc_path = ipc_path
        self.context = zmq.Context()

    def _build_connection_string(self, bind: bool) -> str:
        if self.protocol == ZeroMQProtocol.TCP:
            if bind:
                return f"tcp://*:{self.port}"
            else:
                return f"tcp://localhost:{self.port}"
        elif self.protocol == ZeroMQProtocol.IPC:
            return f"ipc://{self.ipc_path}"

    def stop(self):
        """Stops the server or worker, shutting down the executor and closing the ZMQ context."""
        self.executor.shutdown(wait=True)
        self.context.term()
