from .zero_mq_processing_base import ZeroMQProcessingBase
import zmq
from .utils import create_message, parse_message


class ZeroMQServer(ZeroMQProcessingBase):
    def start(self):
        """Starts the server by binding to a port and entering the message processing loop."""
        socket = self.context.socket(zmq.REP)
        socket.bind(self._build_connection_string(bind=True))
        print("Server started and bound to port")

        try:
            while True:
                message = socket.recv_multipart()
                print(f"Server received message: {message}")

                if len(message) < 3:
                    print(f"Malformed message received: {message}")
                    continue

                parsed_message = parse_message(message)

                response = self.process_message(parsed_message)
                if response:
                    print(f"Server sending response: {response}")
                    socket.send_multipart(response)
        except zmq.ZMQError as e:
            print(f"ZMQ Error occurred: {e}")
        except Exception as e:
            print(f"Unknown exception occurred: {e}")
        finally:
            self.stop()

    def process_message(self, parsed_message: dict) -> list:
        """Processes individual messages."""
        response_data = self.handle_message(parsed_message)
        event_name = parsed_message["event_name"]
        return create_message(event_name, response_data)
