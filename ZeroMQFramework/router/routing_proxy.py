import zmq
from loguru import logger
from ..router.routing_strategy import ZeroMQRoutingStrategy


class ZeroMQRoutingProxy(ZeroMQRoutingStrategy):
    def __init__(self):
        self.shutdown_requested = False

    def route(self, frontend_socket: zmq.Socket, backend_socket: zmq.Socket, poller: zmq.Poller = None,
              poll_timeout: int = 1000):

        while not self.shutdown_requested:
            socks = dict(poller.poll(poll_timeout))
            if frontend_socket in socks and socks[frontend_socket] == zmq.POLLIN:
                message = frontend_socket.recv_multipart()
                backend_socket.send_multipart(message)

            if backend_socket in socks and socks[backend_socket] == zmq.POLLIN:
                message = backend_socket.recv_multipart()
                frontend_socket.send_multipart(message)

    def shutdown_routing(self):
        self.shutdown_requested = True
        logger.info("Shutting down routing proxy...")
