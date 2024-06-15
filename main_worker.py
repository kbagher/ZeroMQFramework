from ZeroMQFramework import *
from typing import Any
import json


class MyWorker(ZeroMQWorker):
    def handle_message(self, message: dict) -> Any:
        print(f"MyWorker received: {message}")
        data = {
            "name": "Krishna",
            "Course": "DSA",
            "Batch": "July_2023"
        }
        json_array = json.dumps(data)
        print(f"MyWorker returning: {json_array}")

        return message

if __name__ == "__main__":
    port = 5556  # Backend port
    worker = MyWorker(port, protocol=ZeroMQProtocol.TCP, max_threads=5)
    worker.start()
