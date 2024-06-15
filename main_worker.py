from ZeroMQFramework import *
from typing import Any
import json
import threading
import zmq


class MyWorker(ZeroMQWorker, threading.Thread):
    def __init__(self, port: int, protocol: ZeroMQProtocol, address: str = "localhost"):
        ZeroMQWorker.__init__(self, port, protocol, address)
        threading.Thread.__init__(self)
        self.daemon = True  # Optional: makes the thread a daemon thread

    def handle_message(self, message: dict) -> Any:
        print(f"MyWorker received: {message}")
        data = {
            "name": "Krishna",
            "Course": "DSA",
            "Batch": "July_2023"
        }
        json_array = json.dumps(data)
        print(f"MyWorker returning: {json_array}")
        return [message]

    def run(self):
        self.start_worker()

    def start_worker(self):
        self.socket = self.context.socket(zmq.DEALER)
        connection_string = f"{self.protocol}://{self.address}:{self.port}"
        self.socket.connect(connection_string)
        print(f"Worker connected to {connection_string}")
        self.process_messages()


if __name__ == "__main__":
    port = 5556  # Backend port
    address = "localhost"
    protocol = ZeroMQProtocol.TCP
    max_threads = 5

    threads = []
    for _ in range(max_threads):
        worker = MyWorker(port, protocol, address)
        worker.start()
        threads.append(worker)

    for thread in threads:
        thread.join()
