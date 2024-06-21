from ..helpers.config import ZeroMQConnection


class ZeroMQHeartbeatConfig:
    def __init__(self, connection: ZeroMQConnection, socket_type: int, interval: int = 10, timeout: int = 30, max_missed: int = 3):
        self.connection = connection
        self.socket_type = socket_type
        self.interval = interval
        self.timeout = timeout
        self.max_missed = max_missed
