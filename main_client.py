import random
import string
import sys

import client
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
    logger.warning("Main process received shutdown signal")
    sys.exit(1)


def main():
    setup_logging('logs/client_logs')
    config_file = 'config.ini'

    config = load_config(config_file, "general")
    node_id = config.get('server_host')

    server_host = config.get('server_host')
    server_port = config.get('server_port')
    server_heartbeat_port = config.get('server_heartbeat_port')
    server_heartbeat_host = config.get('server_heartbeat_host')

    client_id = generate_short_udid()

    ipc_path = "/tmp/my_super_app_heartbeat.ipc"
    heartbeat_conn = ZeroMQTCPConnection(port=server_heartbeat_port, host=server_heartbeat_host)
    heartbeat_config = ZeroMQHeartbeatConfig(heartbeat_conn, interval=5)
    connection = ZeroMQTCPConnection(port=server_port, host=server_host)

    client_obj = ZeroMQClient(config_file=config_file, connection=connection, heartbeat_config=None,
                              timeout=5)
    batch_start_time = time.time()

    retry_timeout = 2

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    while True:
        if not client_obj.connect():
            logger.warning("Main: Client connection failed")
            time.sleep(3)
        else:
            break

    try:
        x = 0
        overall_start_time = time.time()
        batch_size = 1000
        while x < 1000000 and (time.time() - overall_start_time) <= 10000:
            try:
                if x % batch_size == 0:
                    batch_start_time = time.time()

                event_name = ZeroMQEvent.MESSAGE.value
                event_data = {"content": f"Message {x} from client {client_id}"}
                response = client_obj.send_message(event_name, event_data)

                reply_content = response["event_data"]["event_data"]["content"]
                received_id = reply_content.split()[-1]

                if x % batch_size == 0:
                    batch_elapsed_time = time.time() - batch_start_time
                    overall_elapsed_time = time.time() - overall_start_time
                    overall_time_formatted = format_time(overall_elapsed_time)
                    logger.info(
                        f"Messages Sent: {x}, Batch time: {batch_elapsed_time:.2f} seconds, Overall time: {overall_time_formatted}")

                x += 1
                # time.sleep(0.2)

            except ZeroMQSocketError as e:
                logger.warning(f"Main: socket error. {e}")
                time.sleep(retry_timeout)
                client_obj.connect()
            except ZeroMQMalformedMessage:
                logger.error(f"Error: Message malformed: {client_id}")
            except ZeroMQTimeoutError:
                logger.warning("No response received within the timeout period, retrying...")
                time.sleep(retry_timeout)
            except ZeroMQConnectionError as e:
                logger.error(f"ZeroMQConnectionError occurred: {e}, reconnecting client.")
                client_obj.connect()
                time.sleep(retry_timeout)
            except ZeroMQClientError as e:
                logger.error(f"============ {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        client_obj.cleanup()

    logger.info(f"Done Sending:")


if __name__ == "__main__":
    main()
