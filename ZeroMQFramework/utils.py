import json


def create_message(event_name: str, event_data: dict) -> list:
    try:
        return [
            # b'',  # Empty Frame
            event_name.encode('utf-8'),  # Event Name
            json.dumps(event_data).encode('utf-8')  # Event Data
        ]
    except Exception as e:
        raise ValueError(f"Error creating message: {e}")


def parse_message(message: list) -> dict:
    if len(message) < 2:
        raise ValueError(f"Malformed message: {message}")
    try:
        if message[0] == b'':  # Case: [empty frame, event name, event data]
            event_name = message[1].decode('utf-8')
            event_data = json.loads(message[2].decode('utf-8'))
        elif len(message) == 4 and message[1] == b'':  # Case: [address, empty frame, event name, event data]
            event_name = message[2].decode('utf-8')
            event_data = json.loads(message[3].decode('utf-8'))
        else:  # Case: [event name, event data]
            event_name = message[0].decode('utf-8')
            event_data = json.loads(message[1].decode('utf-8'))
        return {
            "event_name": event_name,
            "event_data": event_data
        }
    except Exception as e:
        raise ValueError(f"Error parsing message: {e}")

# def build_connection_string(bind: bool, protocol: ZeroMQProtocol, port: int, ipc_path: str = "/tmp/zmq.ipc") -> str:
#     if protocol == ZeroMQProtocol.TCP:
#         if bind:
#             return f"tcp://*:{port}"
#         else:
#             return f"tcp://localhost:{port}"
#     elif protocol == ZeroMQProtocol.IPC:
#         return f"ipc://{ipc_path}"
