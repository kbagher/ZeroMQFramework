import zmq
from loguru import logger
import time
from ZeroMQFramework import ZeroMQSocketMonitor


class ZeroMQClient:
    def __init__(self, connect_address):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(connect_address)
        self.monitor = ZeroMQSocketMonitor(self.context, self.socket)
        self.monitor.start()

    def send_message(self, message):
        if self.monitor.is_connected():
            try:
                logger.info(f"Client sending: {message}")
                self.socket.send_string(message)
                reply = self.socket.recv_string()
                logger.info(f"Client received: {reply}")
            except Exception as e:
                logger.error(f"Client exception: {e}")
        else:
            logger.error("Client error: Socket not connected")

    def close(self):
        self.monitor.stop()
        self.socket.close()
        self.context.term()

    def signal_handler(signum, frame):
        print("Interrupt signal received. Shutting down...")
        # Perform your cleanup here, if necessary
        # For example, you can set a shutdown flag that your main program checks


if __name__ == "__main__":
    server_address = "tcp://127.0.0.1:5555"
    client = ZeroMQClient(server_address)

    try:
        for i in range(10000):
            client.send_message(f"Message {i}")
            time.sleep(0.1)  # Short delay between messages
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        client.close()
