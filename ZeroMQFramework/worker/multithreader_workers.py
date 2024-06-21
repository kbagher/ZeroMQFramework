from ZeroMQFramework import *
from ZeroMQFramework import ZeroMQHeartbeatConfig


class ZeroMQMultiThreadedWorkers:
    def __init__(self, connection: ZeroMQConnection, num_workers: int = 1,
                 handle_message_factory: Callable[[], Callable[[dict], Any]] = None, heartbeat_config: ZeroMQHeartbeatConfig = None):
        self.connection = connection
        self.num_workers = num_workers
        self.heartbeat_config = heartbeat_config
        self.workers = []
        self.handle_message_factory = handle_message_factory
        self.shutdown_requested = False
        self.context = zmq.Context()  # Shared context for all workers

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def start(self):
        for _ in range(self.num_workers):
            handle_message = self.handle_message_factory() if self.handle_message_factory else None
            worker = ZeroMQWorker(self.connection, handle_message, self.context, heartbeat_config=self.heartbeat_config)
            worker.start()
            self.workers.append(worker)
        Debug.info(f"{self.num_workers} workers started.")

    def request_shutdown(self, signum, frame):
        Debug.warn("Received shutdown signal, stopping all workers...")
        self.shutdown_requested = True
        for worker in self.workers:
            worker.request_shutdown(signum, frame)
        for worker in self.workers:
            worker.join()
        self.cleanup()
        Debug.info("All workers have been stopped.")

    def cleanup(self):
        if not self.context.closed:
            self.context.term()

