import uuid
import zmq.utils.monitor
import zmq
from threading import Thread
from ..helpers.logger import logger


class ZeroMQSocketMonitor:
    def __init__(self, context: zmq.Context, socket: zmq.Socket):
        self.context = context
        self.socket = socket
        self.monitor_socket = None
        self.running = False
        self.connected = False
        self.monitor_thread = None

    def start(self):
        self.running = True
        tmp_id = uuid.uuid4().hex[:8]
        self.socket.monitor(f"inproc://monitor.sock", zmq.EVENT_ALL)
        self.monitor_socket = self.context.socket(zmq.PAIR)
        self.monitor_socket.connect(f"inproc://monitor.sock")
        self.monitor_thread = Thread(target=self.monitor_events, daemon=True)
        self.monitor_thread.start()

    def stop(self):
        self.running = False
        if self.monitor_thread is not None:
            self.monitor_thread.join()  # Ensure the thread has finished
        # self.cleanup()

    def monitor_events(self):
        poller = zmq.Poller()
        poller.register(self.monitor_socket, zmq.POLLIN)

        while self.running:
            socks = dict(poller.poll(timeout=3000))  # Poll with a timeout, will have to make it a config or anything
            if self.monitor_socket in socks:
                try:
                    event = self.monitor_socket.recv_multipart(flags=zmq.NOBLOCK)
                    event_dict = zmq.utils.monitor.parse_monitor_message(event)
                    event_type = event_dict['event']
                    if event_type == zmq.EVENT_CONNECTED:
                        self.connected = True
                        logger.info("Socket monitor: Connected")
                    # elif event_type == zmq.EVENT_DISCONNECTED or event_type == zmq.EVENT_CLOSED:
                    elif event_type == zmq.EVENT_DISCONNECTED:
                        self.connected = False
                        logger.info("Socket monitor: Disconnected")
                except zmq.error.Again as e:
                    pass  # Handle non-blocking receive timeout
                except Exception as e:
                    logger.error("Socket monitor exception:", e)
        self.cleanup()

    def cleanup(self):
        if self.monitor_socket:
            self.monitor_socket.close()

    def is_connected(self):
        return self.connected
