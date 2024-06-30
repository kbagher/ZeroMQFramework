from abc import ABC, abstractmethod

import zmq

from ZeroMQFramework import ZeroMQConnection


class ZeroMQRoutingStrategy(ABC):
    @abstractmethod
    def route(self, frontend_socket: zmq.Socket, backend_socket: zmq.Socket, poller: zmq.Poller = None,
              poll_timeout: int = 1000):
        pass

    @abstractmethod
    def shutdown_routing(self):
        pass
