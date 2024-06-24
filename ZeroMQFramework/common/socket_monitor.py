import uuid
import zmq.utils.monitor
import zmq
from threading import Thread, Event, Lock
from loguru import logger
import atexit


class ZeroMQSocketMonitor:
    _cleanup_registered = False

    def __init__(self, context: zmq.Context, socket: zmq.Socket, on_socket_closed_callback=None,
                 on_socket_connect_callback=None, on_socket_disconnect_callback=None):
        self.context = context
        self.socket = socket
        self.monitor_socket = None
        self.running_event = Event()
        self.reset_socket_event = Event()
        self.connected = False
        self.monitor_thread = None
        self.lock = Lock()
        self.poller = zmq.Poller()
        self.poll_timeout = 500
        self.on_socket_closed_callback = on_socket_closed_callback  # inform the main class about socket status
        self.on_socket_connect_callback = on_socket_connect_callback  # inform the main class about socket status
        self.on_socket_disconnect_callback = on_socket_disconnect_callback  # inform the main class about socket status

        if not ZeroMQSocketMonitor._cleanup_registered:
            atexit.register(self.cleanup)
            ZeroMQSocketMonitor._cleanup_registered = True

    def start(self):
        try:
            if self.monitor_socket is None:
                self.running_event.set()
                self._initialize_monitor()
                self.monitor_thread = Thread(target=self.monitor_events, daemon=True)
                self.monitor_thread.start()
        except Exception as e:
            logger.error(f"Socket monitor: Failed to start monitor thread: {e}")
            self.cleanup()  # Ensure cleanup if starting the thread fails

    def stop(self):
        self.running_event.clear()
        self.cleanup()

    def reset_socket(self, new_socket):
        self.reset_socket_event.set()
        self.socket = new_socket
        self._initialize_monitor()
        self.reset_socket_event.clear()

    def _initialize_monitor(self):
        tmp_id = uuid.uuid4().hex[:8]
        self.socket.monitor(f"inproc://{tmp_id}.sock", zmq.EVENT_ALL)
        self.monitor_socket = self.context.socket(zmq.PAIR)
        self.monitor_socket.connect(f"inproc://{tmp_id}.sock")

    def monitor_events(self):
        self.poller.register(self.monitor_socket, zmq.POLLIN)

        while self.running_event.is_set():
            if self.reset_socket_event.is_set():
                continue
            socks = dict(self.poller.poll(timeout=self.poll_timeout))
            if self.monitor_socket in socks:
                try:
                    event = self.monitor_socket.recv_multipart(flags=zmq.NOBLOCK)
                    event_dict = zmq.utils.monitor.parse_monitor_message(event)
                    event_type = event_dict['event']
                    with self.lock:
                        if event_type == zmq.EVENT_CONNECTED:
                            self.connected = True
                            logger.info("Socket monitor: Connected")
                            if self.on_socket_connect_callback:
                                self.on_socket_connect_callback()
                        elif event_type == zmq.EVENT_DISCONNECTED:
                            self.connected = False
                            logger.info("Socket monitor: Disconnected")
                            if self.on_socket_disconnect_callback:
                                self.on_socket_disconnect_callback()
                        elif event_type == zmq.EVENT_CLOSED:
                            self.connected = False
                            logger.warning("Socket monitor: Closed. Make sure you call reset_socket() after "
                                           "reconnecting (or reinitialising) the main socket that is being monitored")
                            if self.on_socket_closed_callback:
                                self.on_socket_closed_callback()
                except zmq.error.Again:
                    pass  # Handle non-blocking receive timeout
                except Exception as e:
                    logger.error(f"Socket monitor exception: {e}")

    def cleanup(self):
        self.running_event.clear()
        if self.monitor_thread is not None and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=self.poll_timeout + 1000)
            if self.monitor_thread.is_alive():
                logger.warning("Socket Monitor: Monitor thread did not terminate; may require forced shutdown.")
            self.monitor_thread = None
        if self.monitor_socket:
            self.poller.unregister(self.monitor_socket)
            self.monitor_socket.close()
            self.monitor_socket = None
        with self.lock:
            self.connected = False

    def is_connected(self):
        with self.lock:
            return self.connected
