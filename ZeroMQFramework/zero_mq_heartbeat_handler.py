import time
import threading
import queue


class ZeroMQHeartbeatHandler:
    """
    HeartbeatHandler manages worker heartbeats to ensure that workers are still connected
    and responsive. It processes heartbeat messages asynchronously and monitors the
    heartbeat status of each worker in a separate thread.

    Attributes:
        interval (int): The interval in seconds at which the heartbeat monitoring thread runs.
        timeout (int): The maximum time in seconds allowed between heartbeats before a worker is considered missing.
        max_missed (int): The maximum number of missed heartbeats before a worker is marked as disconnected.
        worker_heartbeats (dict): A dictionary mapping worker IDs to their last heartbeat timestamp and missed count.
        running (bool): A flag indicating whether the HeartbeatHandler is running.
        heartbeat_queue (queue.Queue): A queue for processing incoming heartbeat messages asynchronously.
        lock (threading.Lock): A lock to ensure thread-safe access to worker_heartbeats.
    """

    def __init__(self, interval: int = 10, timeout: int = 30, max_missed: int = 3):
        """
        Initializes the HeartbeatHandler with the specified interval, timeout, and max_missed values.

        Args:
            interval (int): The interval in seconds at which the heartbeat monitoring thread runs.
            timeout (int): The maximum time in seconds allowed between heartbeats before a worker is considered missing.
            max_missed (int): The maximum number of missed heartbeats before a worker is marked as disconnected.
        """
        self.worker_heartbeats = {}
        self.interval = interval
        self.timeout = timeout
        self.max_missed = max_missed
        self.running = True
        self.heartbeat_queue = queue.Queue()
        self.lock = threading.Lock()  # Add a lock for thread safety

    def start(self):
        """
        Starts the HeartbeatHandler by launching the heartbeat processing and monitoring threads.
        """
        threading.Thread(target=self._process_heartbeats, daemon=True).start()
        threading.Thread(target=self._monitor_heartbeats, daemon=True).start()

    def stop(self):
        """
        Stops the HeartbeatHandler by setting the running flag to False.
        """
        self.running = False

    def handle_heartbeat(self, worker_id):
        """
        Handles an incoming heartbeat message by placing the worker ID into the heartbeat queue.

        Args:
            worker_id (str): The ID of the worker sending the heartbeat.
        """
        self.heartbeat_queue.put(worker_id)

    def _process_heartbeats(self):
        """
        Processes heartbeat messages from the heartbeat queue and updates the worker_heartbeats dictionary.
        This method runs in a separate thread.
        """
        while self.running:
            try:
                worker_id = self.heartbeat_queue.get(timeout=1)
                with self.lock:
                    if worker_id not in self.worker_heartbeats:
                        print(f"Worker {worker_id} is connected for the first time.")
                    self.worker_heartbeats[worker_id] = (time.time(), 0)  # Reset missed count to 0
            except queue.Empty:
                continue

    def _monitor_heartbeats(self):
        """
        Monitors the worker_heartbeats dictionary for any workers that have missed too many heartbeats.
        Workers that have missed too many heartbeats are marked as disconnected and removed from the dictionary.
        This method runs in a separate thread.
        """
        while self.running:
            current_time = time.time()
            with self.lock:
                for worker_id, (last_heartbeat, missed_count) in list(self.worker_heartbeats.items()):
                    if current_time - last_heartbeat > self.timeout:
                        missed_count += 1
                        if missed_count > self.max_missed:
                            disconnected_duration = current_time - last_heartbeat
                            print(f"Worker {worker_id} missed too many heartbeats, marking as disconnected. "
                                  f"Last heartbeat was {disconnected_duration:.2f} seconds ago.")
                            del self.worker_heartbeats[worker_id]
                        else:
                            self.worker_heartbeats[worker_id] = (last_heartbeat, missed_count)
            time.sleep(self.interval)  # Sleep for the interval before next check
