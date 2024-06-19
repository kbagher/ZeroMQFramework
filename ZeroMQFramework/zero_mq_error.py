class ZeroMQClientError(Exception):
    pass


class ZeroMQConnectionError(ZeroMQClientError):
    pass


class ZeroMQTimeoutError(ZeroMQClientError):
    pass


class ZeroMQError(Exception):
    pass


class ZeroMQMalformedMessage(ZeroMQError):
    pass
