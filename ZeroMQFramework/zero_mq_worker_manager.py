from typing import Callable, Any
import signal
from ZeroMQFramework import ZeroMQWorker, ZeroMQProtocol


class ZeroMQWorkerManager:
    def __init__(self, port: int, protocol: ZeroMQProtocol = ZeroMQProtocol.TCP, address: str = "localhost", num_workers: int = 1, handle_message: Callable[[dict], Any] = None):
        self.port = port
        self.protocol = protocol
        self.address = address
        self.num_workers = num_workers
        self.workers = []
        self.handle_message = handle_message
        self.shutdown_requested = False

        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def start(self):
        for _ in range(self.num_workers):
            worker = ZeroMQWorker(self.port, self.protocol, self.address, self.handle_message)
            worker.start()
            self.workers.append(worker)
        print(f"{self.num_workers} workers started.")

    def request_shutdown(self, signum, frame):
        print("Received shutdown signal, stopping all workers...")
        self.shutdown_requested = True
        for worker in self.workers:
            worker.request_shutdown(signum, frame)
        for worker in self.workers:
            worker.join()
        print("All workers have been stopped.")
