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
    logger.warn("Main process received shutdown signal")
    sys.exit(0)


def main():
    logger.configure_logger('logs/client_logs')
    client_id = generate_short_udid()

    ipc_path = "/tmp/my_super_app_heartbeat.ipc"  # IPC path, make sure it's unique for each application.
    heartbeat_conn = ZeroMQIPCConnection(ipc_path=ipc_path)
    # heartbeat_config = ZeroMQHeartbeatConfig(heartbeat_conn, interval=5)
    heartbeat_config = None

    client = ZeroMQClient(port=5555, host='localhost', heartbeat_config=heartbeat_config, protocol=ZeroMQProtocol.TCP,
                          timeout=5000, retry_attempts=3,
                          retry_timeout=1000)
    batch_start_time = time.time()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        client.connect()
        x = 0
        overall_start_time = time.time()  # Record the overall start time
        batch_size = 100
        while x < 1000000 and (time.time() - overall_start_time) <= 10:
            try:
                if x % batch_size == 0:
                    batch_start_time = time.time()  # Record the start time for the batch

                try:
                    event_name = ZeroMQEvent.MESSAGE.value
                    event_data = {"content": f"Message {x} from client {client_id}"}
                    # event_data = {"delivery_tag":1,"event":"Starting new HTTP connection (1): overallsentiment.eu-west-1.aws.lucidya.production:80","event_name":"alert_data","filename":"connectionpool.py","func_name":"_new_conn","level":"debug","level_number":10,"lineno":244,"logger":"urllib3.connectionpool","message_metadata":"331abd3cd569ac7b85d1a9b4a05aa4bb8428a85c6303193ff298f7eb41733878","module":"connectionpool","parent_span_id":"8a9a4f584a247782","pathname":"/usr/local/lib/python3.11/site-packages/urllib3/connectionpool.py","process":1,"process_name":"MainProcess","span_id":"cefccc79b8a4ea1e","thread":139790866884288,"thread_name":"ThreadPoolExecutor-1_2","trace_id":"eb146e38420d13700ae69102fd244531"}
                    response = client.send_message(event_name, event_data)
                    # print(response)
                    # reply_content = response["event_data"][0]["event_data"]["content"]
                    reply_content = response["event_data"]["event_data"]["content"]
                    # print(reply_content)
                    received_id = reply_content.split()[-1]
                except Exception as e:
                    logger.error(e)
                    pass

                if x % batch_size == 0:
                    batch_elapsed_time = time.time() - batch_start_time  # Calculate elapsed time for the batch
                    overall_elapsed_time = time.time() - overall_start_time  # Calculate overall elapsed time
                    overall_time_formatted = format_time(overall_elapsed_time)
                    logger.info(
                        f"Messages Sent: {x}, Batch time: {batch_elapsed_time:.2f}"
                        f"seconds, Overall time: {overall_time_formatted}",
                        mode=LogMode.UPDATE)
                # if received_id != client_id:
                #     Debug.error(f"Error: Mismatched client ID in reply. Sent: {client_id}, Received: {received_id}")
                # if int(reply_content.split()[1]) != x:
                #     Debug.error(f"Error: Mismatched value. Sent: {x}, Received: {reply_content.split()[1]}")
                x += 1
            except ZeroMQMalformedMessage:
                logger.error(f"Error: Message malformed: {client_id}")
            except ZeroMQTimeoutError:
                logger.warn("No response received within the timeout period, retrying...")
                # Implement retry logic or other actions
                time.sleep(client.retry_timeout / 1000)  # Optional: wait before retrying
            except ZeroMQConnectionError as e:
                logger.error(f"ZeroMQConnectionError occurred: {e}, reconnecting client.")
                client.cleanup()
                client.connect()
                time.sleep(client.retry_timeout / 1000)
    except Exception as e:
        logger.error(f"An unexpected error occurred", e)
    finally:
        client.cleanup()

    logger.info(f"Done Sending:")


if __name__ == "__main__":
    main()
