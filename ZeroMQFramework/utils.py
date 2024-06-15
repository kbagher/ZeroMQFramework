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
    if len(message) < 3 or message[0] != b'':
        raise ValueError(f"Malformed message: {message}")

    try:
        return {
            "event_name": message[1].decode('utf-8'),
            "event_data": json.loads(message[2].decode('utf-8'))
        }
    except Exception as e:
        raise ValueError(f"Error parsing message: {e}")
