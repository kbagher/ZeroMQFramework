class ZeroMQError(Exception):
    """Base class for ZeroMQ errors."""
    pass


class ZeroMQClientError(ZeroMQError):
    """Base class for ZeroMQ client errors."""
    pass


class ZeroMQConnectionError(ZeroMQClientError):
    """Raised when a connection to ZeroMQ fails."""
    pass


class ZeroMQTimeoutError(ZeroMQClientError):
    """Raised when a ZeroMQ operation times out."""
    pass


class ZeroMQSocketError(ZeroMQError):
    """Base class for ZeroMQ socket errors."""
    pass


class ZeroMQQSocketDisconnected(ZeroMQSocketError):
    """Raised when a ZeroMQ socket is disconnected."""
    pass


class ZeroMQQSocketClosed(ZeroMQSocketError):
    """Raised when a ZeroMQ socket is closed."""
    pass


class ZeroMQQSocketInvalid(ZeroMQSocketError):
    """Raised when a ZeroMQ socket is invalid."""
    pass


class ZeroMQMalformedMessage(ZeroMQError):
    """Raised when a received ZeroMQ message is malformed."""
    pass
