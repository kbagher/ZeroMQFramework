from abc import ABC, abstractmethod


class ZeroMQRoutingStrategy(ABC):
    @abstractmethod
    def route(self, frontend_socket, backend_socket):
        pass
