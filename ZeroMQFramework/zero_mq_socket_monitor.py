import zmq
import zmq.asyncio
from zmq.utils.monitor import parse_monitor_message
import threading
from .debug import Debug
from .config import ZeroMQConnection
from enum import IntFlag
import struct


class ZeroMQMonitorEvents(IntFlag):
    EVENT_CONNECTED = zmq.EVENT_CONNECTED
    EVENT_CONNECT_DELAYED = zmq.EVENT_CONNECT_DELAYED
    EVENT_CONNECT_RETRIED = zmq.EVENT_CONNECT_RETRIED
    EVENT_LISTENING = zmq.EVENT_LISTENING
    EVENT_BIND_FAILED = zmq.EVENT_BIND_FAILED
    EVENT_ACCEPTED = zmq.EVENT_ACCEPTED
    EVENT_ACCEPT_FAILED = zmq.EVENT_ACCEPT_FAILED
    EVENT_CLOSED = zmq.EVENT_CLOSED
    EVENT_CLOSE_FAILED = zmq.EVENT_CLOSE_FAILED
    EVENT_DISCONNECTED = zmq.EVENT_DISCONNECTED
    EVENT_MONITOR_STOPPED = zmq.EVENT_MONITOR_STOPPED
    EVENT_HANDSHAKE_FAILED_NO_DETAIL = zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL
    EVENT_HANDSHAKE_SUCCEEDED = zmq.EVENT_HANDSHAKE_SUCCEEDED
    EVENT_HANDSHAKE_FAILED_PROTOCOL = zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL
    EVENT_HANDSHAKE_FAILED_AUTH = zmq.EVENT_HANDSHAKE_FAILED_AUTH


class SocketMonitor(threading.Thread):
    def __init__(self, context: zmq.Context, connection: ZeroMQConnection):
        super().__init__()
        self.context = context
        self.connection = connection
        self.monitor_socket = None
        self.connected = False
        self.current_state = None
        self.running = False
        self.poller = zmq.Poller()

    def setup_monitor(self):
        monitor_url = self.connection.get_connection_string(bind=False) + ".monitor"
        self.monitor_socket = self.context.socket(zmq.PAIR)
        self.monitor_socket.connect(monitor_url)
        self.poller.register(self.monitor_socket, zmq.POLLIN)
        return monitor_url

    def run(self):
        self.running = True
        while self.running:
            print("in while")
            try:
                socks = dict(self.poller.poll(100))
                if self.monitor_socket in socks and socks[self.monitor_socket] == zmq.POLLIN:
                    event = self.monitor_socket.recv_multipart()
                    if event:
                        ee = parse_monitor_message(event)
                        # self.handle_event(ee)
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    break  # Context terminated
                elif e.errno == zmq.ENOTSOCK:
                    break  # Socket closed
                else:
                    Debug.error(f"Error in monitoring socket", e)
                    break
        self.cleanup()

    def handle_event(self, event_id):
        if event_id & zmq.EVENT_CONNECTED:
            self.connected = True
            Debug.info("Socket is now connected.")
        elif event_id & zmq.EVENT_DISCONNECTED:
            self.connected = False
            Debug.warn("Socket is disconnected.")
        self.current_state = event_id

    def is_connected(self):
        return self.connected

    def get_current_state(self):
        return self.current_state

    def cleanup(self):
        if self.monitor_socket:
            self.monitor_socket.close()
        self.poller.unregister(self.monitor_socket)
        Debug.info("Stopped ZeroMQ monitor socket.")

    def stop(self):
        """
        Stops the HeartbeatHandler by setting the running flag to False.
        """
        self.running = False