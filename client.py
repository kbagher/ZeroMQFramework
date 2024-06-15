import zmq


def client(port, num_requests=10):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://localhost:{port}")

    for i in range(num_requests):
        message = f"Hello {i}"
        print(f"Client sending: {message}")
        socket.send_string(message)
        reply = socket.recv_string()
        print(f"Client received: {reply}")

    socket.close()
    context.term()
