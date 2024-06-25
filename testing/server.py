import zmq
from threading import Thread
from loguru import logger


class ZeroMQServer:
    def __init__(self, bind_address):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(bind_address)

    def run(self):
        try:
            while True:
                message = self.socket.recv_string()
                logger.info(f"Server received: {message}")
                self.socket.send_string(f"Reply to {message}")
        except Exception as e:
            logger.error(f"Server exception: {e}")
        finally:
            self.socket.close()
            self.context.term()


if __name__ == "__main__":
    server_address = "tcp://127.0.0.1:5555"
    server = ZeroMQServer(server_address)
    server_thread = Thread(target=server.run)
    server_thread.start()
    server_thread.join()
