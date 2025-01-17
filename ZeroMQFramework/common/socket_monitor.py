import zmq.utils.monitor
import zmq
from threading import Thread, Event, Lock
from loguru import logger
import atexit
from ..helpers.utils import *


class ZeroMQSocketMonitor:
    _cleanup_registered = False

    def __init__(self, context: zmq.Context, socket: zmq.Socket, on_socket_closed_callback=None,
                 on_socket_connect_callback=None, on_socket_disconnect_callback=None):
        self.context = context
        self.socket = socket
        self.monitor_socket = None
        self.running_event = Event()
        self.reset_socket_event = Event()
        self.stop_warnings = Event()
        self._is_connected = False
        self.monitor_thread = None
        self.lock = Lock()
        self.poller = zmq.Poller()
        self.poll_timeout = 500  # in ms
        self.on_socket_closed_callback = on_socket_closed_callback  # inform the main class about socket status
        self.on_socket_connect_callback = on_socket_connect_callback  # inform the main class about socket status
        self.on_socket_disconnect_callback = on_socket_disconnect_callback  # inform the main class about socket status

        if not ZeroMQSocketMonitor._cleanup_registered:
            atexit.register(self.cleanup)
            ZeroMQSocketMonitor._cleanup_registered = True

    def start(self):
        """
        Start the monitor thread.

        :return: None
        """
        try:
            if self.monitor_socket is None:  # avoid creating multiple threads
                logger.info("starting monitor thread")
                self.running_event.set()
                self._initialize_monitor()
                self.monitor_thread = Thread(target=self.monitor_events, daemon=True)
                self.monitor_thread.start()
        except Exception as e:
            logger.error(f"Socket monitor: Failed to start monitor thread: {e}")
            self.cleanup()  # Ensure cleanup if starting the thread fails

    def stop(self):
        """
        Stops the monitor and performs necessary cleanup.

        :return: None.
        """
        self.running_event.clear()
        self.cleanup()

    def reset_socket(self, new_socket):
        """
        Reset the socket that is being monitored.
        This is to be used if the socket that is being monitored has been reinitialized.

        :param new_socket: The new socket object to be used for monitoring.
        :return: None
        """
        logger.info("Resetting socket monitor")
        self.reset_socket_event.set()
        time.sleep(self.poll_timeout / 1000)  # Convert to seconds, ensure poller finish its current polling
        self.cleanup_monitor_socket()  # Ensure the old monitor socket is cleaned up
        self.socket = new_socket
        self._initialize_monitor()
        logger.debug("Socket monitor initialised")
        self.reset_socket_event.clear()
        self.stop_warnings.clear()
        logger.debug("Socket monitor reset complete")

    def _initialize_monitor(self):
        """
        Initializes the monitor for the socket.

        :return:
            This method does not return anything.
        """
        logger.debug("Initializing monitor socket")
        tmp_id = get_uuid_hex(8)
        self.socket.monitor(f"inproc://{tmp_id}.sock", zmq.EVENT_ALL)
        self.monitor_socket = self.context.socket(zmq.PAIR)
        self.monitor_socket.connect(f"inproc://{tmp_id}.sock")
        logger.debug(f"Monitor socket connected to inproc://{tmp_id}.sock")
        self.poller.register(self.monitor_socket, zmq.POLLIN)
        logger.debug(f"Monitor socket poll registered: {self.monitor_socket}")

    def cleanup_monitor_socket(self):
        """
        Clean up the current monitor socket.
        """
        if self.monitor_socket:
            logger.debug("Cleaning up monitor socket")
            if self.monitor_socket in self.poller.sockets:
                self.poller.unregister(self.monitor_socket)
                logger.debug("Unregistered monitor socket poller")
            self.monitor_socket.close()
            self.monitor_socket = None
            logger.debug("Monitor socket closed")

    def monitor_events(self):

        while self.running_event.is_set():
            if self.reset_socket_event.is_set():
                continue

            socks = dict(self.poller.poll(timeout=self.poll_timeout))  # ms
            if self.monitor_socket in socks:
                try:
                    event = self.monitor_socket.recv_multipart(flags=zmq.NOBLOCK)
                    event_dict = zmq.utils.monitor.parse_monitor_message(event)
                    event_type = event_dict['event']
                    with self.lock:
                        if event_type == zmq.EVENT_CONNECTED:
                            self._is_connected = True
                            logger.debug("socket connected")
                            self.stop_warnings.clear()
                            if self.on_socket_connect_callback:
                                self.on_socket_connect_callback()
                        elif event_type == zmq.EVENT_DISCONNECTED:
                            self._is_connected = False
                            logger.debug("socket disconnected")
                            if self.on_socket_disconnect_callback:
                                self.on_socket_disconnect_callback()
                        elif event_type == zmq.EVENT_CLOSED:
                            self._is_connected = False
                            if not self.stop_warnings.is_set():
                                logger.debug("socket closed. If the monitored socket is reinitialised, "
                                             "make sure you call reset_socket() to set the new socket object")
                                self.stop_warnings.set()  # to avoid repeated printing. Remove if not needed
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
        self.cleanup_monitor_socket()
        with self.lock:
            self._is_connected = False

    def is_connected(self):
        with self.lock:
            return self._is_connected
