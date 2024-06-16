import zmq
from concurrent.futures import ThreadPoolExecutor
from .config import ZeroMQProtocol
from typing import Any


class ZeroMQBase:
    def __init__(self, port: int, protocol: ZeroMQProtocol = ZeroMQProtocol.TCP):
        self.protocol = protocol.value
        self.port = port
        self.context = zmq.Context()

    def _build_connection_string(self, bind: bool) -> str:
        if bind:
            return f"{self.protocol}://*:{self.port}"
        else:
            return f"{self.protocol}://localhost:{self.port}"

    def stop(self):
        """Stops the server or worker, shutting down the executor and closing the ZMQ context."""
        self.executor.shutdown(wait=True)
        self.context.term()
