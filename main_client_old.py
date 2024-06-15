import zmq
from ZeroMQFramework.utils import *
import random
import string
import json
import time

context = zmq.Context()


def generate_short_udid(length=6):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

context = zmq.Context()

def connect_socket():
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")
    socket.setsockopt(zmq.RCVTIMEO, 5000)
    return socket



def main():
    client_id = generate_short_udid()
    socket = connect_socket()

    timeout =5;
    # context = zmq.Context()
    # socket = context.socket(zmq.REQ)
    # socket.setsockopt(zmq.RCVTIMEO, 5000)
    # socket.connect("tcp://localhost:5555")
    print("Client connected to router")
    x = 0
    while x < 1000000:
        try:
            # Create a message using the create_message function
            print(f"Sending:")
            for i in range(x, 1000000):
                event_name = "Message"
                event_data = {"content": f"Message {i} from client {client_id}"}
                message = create_message(event_name, event_data)
                print(f"Sending: {message}")
                socket.send_multipart(message)
                reply = socket.recv_multipart()
                # Decode the second element of the reply
                print(reply)
                reply_json_str = reply[1].decode('utf-8')
                reply_content = json.loads(reply_json_str)
                # Assuming the JSON string is a list with one dictionary element
                message_content = reply_content[0]['event_data']['content']

                received_id = message_content.split()[-1]
                x += 1;
                if x % 10000 == 0:
                    print(x)
                # Check if the ID matches
                if received_id != client_id:
                    print(f"Error: Mismatched client ID in reply. Sent: {client_id}, Received: {received_id}")
                # else:
                #     print(f"Received reply: {reply}")
        except zmq.Again as e:
            print("No response received within the timeout period, retrying...")
            # Implement retry logic or other actions
            time.sleep(timeout)  # Optional: wait before retrying
        except zmq.ZMQError as e:
            print(f"ZMQError occurred: {e}, reconnecting socket.")
            socket.close()
            socket = connect_socket()
            time.sleep(timeout)

    print(f"Done Sending:")


if __name__ == "__main__":
    main()
