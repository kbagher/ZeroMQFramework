from ZeroMQFramework import *

def main():
    port = 5555  # Frontend port for clients
    backend_port = 5556  # Backend port for workers
    protocol = ZeroMQProtocol.TCP
    router = ZeroMQRouter(port, protocol, backend_port=backend_port, backend_protocol=ZeroMQProtocol.TCP)
    router.start()

if __name__ == "__main__":
    main()
