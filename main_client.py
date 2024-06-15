import random
import string
import time
from ZeroMQFramework import *


def generate_short_udid(length=6):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))


def main():
    client_id = generate_short_udid()
    client = ZeroMQClient(port=5555, protocol=ZeroMQProtocol.TCP, timeout=5000, retry_attempts=3, retry_timeout=1000)

    try:
        client.connect()
        x = 0
        while x < 1000000:
            try:

                event_name = "Message"
                event_data = {"content": f"Message {x} from client {client_id}"}
                response = client.send_message(event_name, event_data)
                reply_content = response["event_data"][0]["event_data"]["content"]
                received_id = reply_content.split()[-1]
                x += 1
                if x % 10000 == 0:
                    print(x)
                # Check if the ID matches
                if received_id != client_id:
                    print(f"Error: Mismatched client ID in reply. Sent: {client_id}, Received: {received_id}")
                # else:
                #     print(f"Received reply: {reply}")
            except ZeroMQTimeoutError:
                print("No response received within the timeout period, retrying...")
                # Implement retry logic or other actions
                time.sleep(client.retry_timeout / 1000)  # Optional: wait before retrying
            except ZeroMQConnectionError as e:
                print(f"ZeroMQConnectionError occurred: {e}, reconnecting client.")
                client.cleanup()
                client.connect()
                time.sleep(client.retry_timeout / 1000)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        client.cleanup()

    print(f"Done Sending:")


if __name__ == "__main__":
    main()
