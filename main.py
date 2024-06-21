# import struct
#
# def decode_event(data):
#     # Unpack the binary data based on ZMQ event structure
#     event_id, endpoint = struct.unpack('!H', data[0:2]), data[2:].decode('utf-8')
#     return event_id, endpoint
#
# byte_array = b'\x04\x00\xad\x00\x00\x00'
# event_id, endpoint = decode_event(byte_array)
# print(f'Event ID: {event_id}, Endpoint: {endpoint}')
import uuid

a = uuid.uuid4().hex[:8]
print(a)