from enum import Enum


class ZeroMQEvent(Enum):
    HEARTBEAT = "heartbeat"
    MESSAGE = "message"
    RESPONSE = "response"
