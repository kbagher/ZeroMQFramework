import zmq
from concurrent.futures import ThreadPoolExecutor
from .config import ZeroMQProtocol
from typing import Any


class ZeroMQBase:
    def __init__(self, port: int, protocol: ZeroMQProtocol = ZeroMQProtocol.TCP, max_threads: int = 5):
        self.protocol = protocol.value
        self.port = port
        self.max_threads = max_threads
        self.context = zmq.Context()
        self.executor = ThreadPoolExecutor(max_workers=self.max_threads)

    def _build_connection_string(self, bind: bool) -> str:
        if bind:
            return f"{self.protocol}://*:{self.port}"
        else:
            return f"{self.protocol}://localhost:{self.port}"

    def stop(self):
        """Stops the server or worker, shutting down the executor and closing the ZMQ context."""
        self.executor.shutdown(wait=True)
        self.context.term()
