from enum import Enum
from typing import Optional
from abc import ABC, abstractmethod




class ZeroMQProtocol(Enum):
    TCP = "tcp"  # well, this is a TCP!
    IPC = "ipc"  # For inter-process communication
    INPROC = "inproc"  # For thread-to-thread communication


class ZeroMQConnection(ABC):
    def __init__(self, protocol: ZeroMQProtocol):
        self.protocol = protocol

    @abstractmethod
    def get_connection_string(self, bind: bool) -> str:
        pass


class ZeroMQTCPConnection(ZeroMQConnection):
    def __init__(self, port: int, host: Optional[str] = "localhost"):
        super().__init__(ZeroMQProtocol.TCP)
        if port is None:
            raise ValueError("Port must be specified for TCP protocol.")
        self.port = port
        self.host = host

    def get_connection_string(self, bind: bool) -> str:
        if bind:
            return f"tcp://*:{self.port}"
        else:
            return f"tcp://{self.host}:{self.port}"


class ZeroMQIPCConnection(ZeroMQConnection):
    def __init__(self, ipc_path: str):
        super().__init__(ZeroMQProtocol.IPC)
        if not ipc_path:
            raise ValueError("IPC path must be specified for IPC protocol. Example: '/tmp/zmq.ipc'")
        self.ipc_path = ipc_path

    def get_connection_string(self, bind: bool) -> str:
        return f"ipc://{self.ipc_path}"


class ZeroMQINPROCConnection(ZeroMQConnection):
    def __init__(self, identifier: str):
        super().__init__(ZeroMQProtocol.INPROC)
        if not identifier:
            raise ValueError("A unique identifier must be specified for INPROC protocol. Example: 'thread1'")
        self.identifier = identifier

    def get_connection_string(self, bind: bool) -> str:
        return f"inproc://{self.identifier}"
