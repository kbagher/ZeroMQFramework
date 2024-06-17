from .zero_mq_base import ZeroMQBase
from typing import Any


class ZeroMQProcessingBase:
    def handle_message(self, message: dict) -> Any:
        """This function should be overridden in subclasses to handle incoming messages."""
        raise NotImplementedError("Subclasses must implement this method.")
