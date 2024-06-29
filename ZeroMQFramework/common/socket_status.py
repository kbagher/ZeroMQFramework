from enum import Enum


class ZeroMQSocketStatus(Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CLOSED = "closed"
