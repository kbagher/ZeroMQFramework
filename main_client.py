import random
import string
from ZeroMQFramework import *


def generate_short_udid(length=6):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))


def format_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def signal_handler(signal, frame):
    Debug.warn("Main process received shutdown signal")
    sys.exit(0)


def main():
    client_id = generate_short_udid()
    client = ZeroMQClient(port=5555, host='localhost', protocol=ZeroMQProtocol.TCP, timeout=5000, retry_attempts=3,
                          retry_timeout=1000)
    batch_start_time = time.time()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        client.connect()
        x = 0
        overall_start_time = time.time()  # Record the overall start time
        batch_size = 100
        while x < 1000000:
            try:
                if x % batch_size == 0:
                    batch_start_time = time.time()  # Record the start time for the batch

                event_name = "Message"
                event_data = {"content": f"Message {x} from client {client_id}"}
                response = client.send_message(event_name, event_data)
                reply_content = response["event_data"][0]["event_data"]["content"]
                received_id = reply_content.split()[-1]

                if x % batch_size == 0:
                    batch_elapsed_time = time.time() - batch_start_time  # Calculate elapsed time for the batch
                    overall_elapsed_time = time.time() - overall_start_time  # Calculate overall elapsed time
                    overall_time_formatted = format_time(overall_elapsed_time)
                    Debug.info(
                        f"Messages Sent: {x}, Batch time: {batch_elapsed_time:.2f}"
                        f"seconds, Overall time: {overall_time_formatted}",
                        mode=LogMode.UPDATE)
                if received_id != client_id:
                    Debug.error(f"Error: Mismatched client ID in reply. Sent: {client_id}, Received: {received_id}")
                if int(reply_content.split()[1]) != x:
                    Debug.error(f"Error: Mismatched value. Sent: {x}, Received: {reply_content.split()[1]}")
                x += 1
            except ZeroMQTimeoutError:
                Debug.warn("No response received within the timeout period, retrying...")
                # Implement retry logic or other actions
                time.sleep(client.retry_timeout / 1000)  # Optional: wait before retrying
            except ZeroMQConnectionError as e:
                Debug.error(f"ZeroMQConnectionError occurred: {e}, reconnecting client.")
                client.cleanup()
                client.connect()
                time.sleep(client.retry_timeout / 1000)
    except Exception as e:
        Debug.error(f"An unexpected error occurred", e)
    finally:
        client.cleanup()

    Debug.info(f"Done Sending:")


if __name__ == "__main__":
    main()
