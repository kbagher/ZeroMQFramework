import zmq
from ZeroMQFramework.utils import *
import random
import string
import json


def generate_short_udid(length=6):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))


def main():
    client_id = generate_short_udid()
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")
    print("Client connected to router")

    # Create a message using the create_message function
    print(f"Sending:")

    for i in range(100000):
        event_name = "Message"
        event_data = {"content": f"Message {i} from client {client_id}"}
        message = create_message(event_name, event_data)
        # print(f"Sending: {event_data}")
        socket.send_multipart(message)
        reply = socket.recv_multipart()
        # Decode the second element of the reply
        print(reply)
        reply_json_str = reply[1].decode('utf-8')
        reply_content = json.loads(reply_json_str)
        # Assuming the JSON string is a list with one dictionary element
        message_content = reply_content[0]['event_data']['content']

        received_id = message_content.split()[-1]

        # Check if the ID matches
        if received_id != client_id:
            print(f"Error: Mismatched client ID in reply. Sent: {client_id}, Received: {received_id}")
        # else:
        #     print(f"Received reply: {reply}")
    print(f"Done Sending:")

if __name__ == "__main__":
    main()
