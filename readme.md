# ZeroMQFramework Documentation

## 1. Overview

ZeroMQFramework is a robust and flexible framework designed to simplify the creation of routers, servers, clients, and
workers using ZeroMQ. It supports various communication patterns and protocols, providing developers with a powerful
toolset to build scalable and efficient distributed systems. Key features of the framework include:

- Support for multiple protocols: TCP, IPC, interproc
- Built-in SocketMonitor for monitoring socket events
- Heartbeat mechanism to ensure the liveness of connections
- Customizable routing strategies to suit different application needs

### Purpose

The primary goal of ZeroMQFramework is to provide developers with an easy-to-use framework that abstracts the
complexities of working with ZeroMQ, enabling rapid development of distributed systems. Designed to operate primarily as
a request/reply (REST-like) framework, ZeroMQFramework focuses on simplifying the creation and management of REST
API-like communication patterns.

## 2. Getting Started

### Installation

#### Prerequisites

Before installing ZeroMQFramework, ensure that the following prerequisites are met:

- Python 3.6 or higher
- ZeroMQ library installed on your system

#### Step-by-Step Installation Guide

1. **Install ZeroMQ:**
    - On Ubuntu:
      ```bash
      sudo apt-get update
      sudo apt-get install libzmq3-dev
      ```
    - On macOS:
      ```bash
      brew install zmq
      ```
    - On Windows:
      Download and install ZeroMQ from the [official website](https://zeromq.org/download/).

2. **Install the framework:**
    - Using pip:
      ```bash
      pip install ZeroMQFramework
      ```

## 3. Framework Architecture

### Components Overview

ZeroMQFramework consists of several key components that work together to facilitate communication in distributed
systems:

- **Client:** Sends requests to a server or a router and receives responses.
- **Server:** Listens for incoming client requests and processes them.
- **Router:** Manages connections and routes messages between clients and workers.
- **Worker:** Connects to a router and processes clients' requests through the router.

Each component is designed to be easily configurable and extendable, allowing developers to adopt the framework to their
specific needs.

### Interaction Between Components

ZeroMQFramework supports two modes of operation:

1. **Client-Router-Worker Mode:**
    - Clients send requests to the Router.
    - The Router forwards requests to Workers.
    - Workers process the requests and send responses back through the Router to the Clients.

2. **Client-Server Mode:**
    - Clients send requests directly to the Server.
    - The Server processes the requests and sends responses back to the Clients.

## 4. Components

### Router

The Router component manages connections and routes messages between clients and workers. It acts as an intermediary,
ensuring that requests from clients are forwarded to available workers for processing.

To create a Router with detailed configuration and logging setup:

```python
from ZeroMQFramework import *

# Setup logging to store router logs
setup_logging('logs/router_logs')

# Configuration file for the router
config_file = 'config.ini'

try:
    # Create frontend TCP connection for clients
    frontend_conn = ZeroMQTCPConnection(port=5555)

    # Create backend connection for workers on different port
    backend_conn = ZeroMQTCPConnection(port=5556)

    # Heartbeat configuration
    heartbeat_conn = ZeroMQTCPConnection(port=5557)
    heartbeat_config = ZeroMQHeartbeatConfig(heartbeat_conn, interval=1, timeout=5, max_missed=1)

    # Initialize and start the router
    router = ZeroMQRouter(config_file=config_file, frontend_connection=frontend_conn,
                          backend_connection=backend_conn,
                          heartbeat_config=heartbeat_config)
    router.start()
except Exception as e:
    print(e)
```

#### Code Explanation

- `setup_logging('logs/router_logs')`: Configures logging to store router logs.
- `config_file = 'config.ini'`: Specifies the configuration file for the router. This will be used internally to
  retrieve the `node_id` or generate a new id and save it to the config.
- `frontend_conn = ZeroMQTCPConnection(port=5555)`: Sets up a TCP connection on port 5555 for clients to connect. This
  connection will be used to receive client requests.
- `backend_conn = ZeroMQTCPConnection(port=5556)`: Sets up a TCP connection on port 5556 for workers to connect. This
  connection will be used to forward client requests to workers and receive their responses.
- `heartbeat_conn = ZeroMQTCPConnection(port=5557)`: Sets up a TCP connection on port 5557 for the heartbeat mechanism
  to monitor the health of workers. Client's can also connect to this port for heartbeats. More details about the
  heartbeat can be found in later sections.
- `heartbeat_config = ZeroMQHeartbeatConfig(heartbeat_conn, interval=1, timeout=5, max_missed=1)`: Configures the
  heartbeat mechanism with the specified parameters.
    - `heartbeat_conn`: The connection object used for the heartbeat mechanism.
    - `interval`: The interval (in seconds) between heartbeat messages.
    - `timeout`: The timeout (in seconds) to wait for a heartbeat response before considering the node has missed a
      heartbeat.
    - `max_missed`: The maximum number of missed heartbeat messages before considering the node as disconnected.
- `router = ZeroMQRouter(config_file=config_file, frontend_connection=frontend_conn, backend_connection=backend_conn, heartbeat_config=heartbeat_config)`:
  Initializes the router with the specified configuration, connections, and heartbeat configuration.
    - `config_file`: Path to the configuration file.
    - `frontend_connection`: The connection object used to receive client requests.
    - `backend_connection`: The connection object used to forward client requests to workers and receive their
      responses.
    - `heartbeat_config`: The heartbeat configuration object.
- `router.start()`: Starts the router. This method initiates the router's main loop, where it listens for client
  requests, forwards them to workers, and sends the workers' responses back to the clients.

### Worker and Server

The Worker component connects to a router and processes client requests through the router. It processes client requests
and sends responses back to the clients via the router. Similarly, the Server will act the same. However, the only
different eis that a server will listen to client's requests directly without the router. This is useful if you need a
single server to handle multiple clients (which is sufficient for many cases).

To create a Worker or a Server:

```python
from ZeroMQFramework import *


def main():
    setup_logging('logs/worker_logs')  # Configure logging for the worker
    config_file = 'config.ini'  # Specify the configuration file

    try:
        # Create worker connection
        worker_conn = ZeroMQTCPConnection(port=5556, host='router_address')

        # Initialize and configure the worker
        # note that to create a Serve,r you only pass the node type 
        # ZeroMQNodeType.SERVER and ZeroMQFramework will handle the rest
        worker = ZeroMQWorker(config_file=config_file, connection=worker_conn, node_type=ZeroMQNodeType.WORKER,
                              handle_message=handle_message)

        # Start the worker
        worker.start()
    except Exception as e:
        print(e)


def handle_message(message):
    # Process the received message and generate an appropriate response
    return f"Processed: {message}"


if __name__ == "__main__":
    main()
```

#### Code Explanation

- `setup_logging('logs/worker_logs')`: Configures logging to store worker logs.
- `config_file = 'config.ini'`: Specifies the configuration file for the worker. This will be used internally to
  retrieve the `node_id` or generate a new id and save it to the config.
- `worker_conn = ZeroMQTCPConnection(port=5556, host='router_address')`: Sets up a TCP connection on port 5556 to
  connect to the router address. This connection will be used to communicate with the router.
- `worker = ZeroMQWorker(config_file=config_file, connection=worker_conn, node_type=ZeroMQNodeType.WORKER, handle_message=handle_message)`:
  Initializes the worker with the specified configuration, connection, node type, and message handling function.
    - `config_file`: Path to the configuration file.
    - `connection`: The connection object used to communicate with the router.
    - `node_type`: Specifies the type of node. In this case, it is set to `ZeroMQNodeType.WORKER` as it will connect to
      a router.
    - `handle_message`: A callback function that processes incoming messages and sends them back to the router.
- `worker.start()`: Starts the worker. This method initiates the worker's main loop, where it listens for messages from
  the router, processes them using the `handle_message` function, and sends back responses.

### Client

The Client component sends requests to a server or router and receives responses. It initiates communication and waits
for the server's reply.

To create a basic Client:

```python
from ZeroMQFramework import *

setup_logging('logs/client_logs')  # Configure logging for the client
config_file = 'config.ini'  # Specify the configuration file

try:
    # Create client connection
    client_conn = ZeroMQTCPConnection(port=5555, host='server_or_router_address')

    # Initialize and configure the client
    client = ZeroMQClient(config_file=config_file, connection=client_conn)

    # Connect to the router or server
    client.connect()

    # Send a request to the server
    event_name = ZeroMQEvent.MESSAGE.value  # or simply any event name you will handle
    event_data = {"content": "Hello World!"}
    response = client.send_message(event_name=event_name, event_data=event_data)
    print(f"Received event_name: {response["event_name"]}, and event_data:{response["event_data"]}")


except Exception as e:
    print(e)

```

A better approach is to handle the errors appropriately and resend or reconnect based on the error:

```python
from ZeroMQFramework import *

setup_logging('logs/client_logs')  # Configure logging for the client
config_file = 'config.ini'  # Specify the configuration file

# Create client connection
client_conn = ZeroMQTCPConnection(port=5555, host='server_or_router_address')

# Initialize and configure the client
client = ZeroMQClient(config_file=config_file, connection=client_conn)

while True:
    try:
        # Connect to the router or server
        client.connect()

        # Send a request to the server
        event_name = ZeroMQEvent.MESSAGE.value  # or simply any event name you will handle
        event_data = {"content": "Hello World!"}
        response = client.send_message(event_name=event_name, event_data=event_data)
        print(f"Received event_name: {response['event_name']}, and event_data: {response['event_data']}")

        break  # Exit loop if successful (this is just a sample code)

    except ZeroMQSocketError as e:
        print(f"Socket error: {e}, retrying in 2 seconds...")
        time.sleep(2)  # sleep for 2 seconds
        client.connect()  # connect again
    except ZeroMQMalformedMessage as e:
        print(f"Malformed message error: {e}")
    except ZeroMQTimeoutError as e:
        print(
            f"Timeout error (No response received within the timeout period, retrying): {e}, retrying in 2 seconds...")
        time.sleep(2)  # sleep for 2 seconds
    except ZeroMQConnectionError as e:
        print(f"Connection error: {e}, retrying in 2 seconds...")
        time.sleep(2)
        client.connect()  # connect again
    except ZeroMQClientError as e:
        print(f"Client error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
```

## 5. Supported Patterns

### Request-Reply Pattern

The request-reply pattern, similar to a REST API, is the primary pattern supported by ZeroMQFramework. Clients send
requests to the server or router, and the server or worker processes these requests and sends back responses.

### Supported Protocols

ZeroMQFramework supports three main protocols for communication: TCP, IPC, and INPROC. Each protocol is designed for
different use cases and environments.

#### ZeroMQProtocol

The `ZeroMQProtocol` enum defines the supported protocols:

- `TCP`: Used for network communication over TCP.
- `IPC`: Used for inter-process communication on the same machine.
- `INPROC`: Used for communication between threads within the same process.

##### TCP (Transmission Control Protocol)

Network communication over TCP. Use TCP when you need to communicate between processes on different machines over a
network.

```python
from ZeroMQFramework import ZeroMQTCPConnection

# Create a TCP connection
tcp_conn = ZeroMQTCPConnection(port=5555, host='host_address')
```

TCP is suitable for scenarios where you need reliable, ordered, and error-checked delivery of messages across networked
computers.

##### IPC (Inter-Process Communication)

Inter-process communication on the same machine. So it's better to use IPC over TCP when you need to communicate between
processes running on the same machine.

```python
from ZeroMQFramework import ZeroMQIPCConnection

# Create an IPC connection
# Make sure the path it's unique for each application. For example,
# you should use the same ipc when connecting routers with workers on the same machine for application A, but use another path for application B
ipc_conn = ZeroMQIPCConnection(ipc_path='/tmp/my_super_app.ipc')
```

## 6.Heartbeat Mechanism

The heartbeat mechanism in ZeroMQFramework ensures the liveness of connections by periodically sending heartbeat
messages. This helps in detecting and handling broken or unresponsive connections. The heartbeat can operate in two
modes: sender and receiver. The mode is determined internally based on the component type.

- **Client and Worker**: Always operate as heartbeat senders.
- **Router and Server**: Always operate as heartbeat receivers.

### How to Configure and Use Heartbeat

To configure and use the heartbeat mechanism, create a heartbeat connection and configure its parameters. Then, pass
the `ZeroMQHeartbeatConfig` object during the initialization of a client, router, worker, or server.

**Example: Creating and Passing Heartbeat Configuration**

```python
from ZeroMQFramework import *

# Create connection
conn = ZeroMQTCPConnection(port=5555)

# Heartbeat configuration
heartbeat_conn = ZeroMQTCPConnection(port=5557)
heartbeat_config = ZeroMQHeartbeatConfig(heartbeat_conn, interval=1, timeout=5, max_missed=1)

# Initialize and configure with heartbeat
# Same approach fo other components (Server, worker and router)
client = ZeroMQClient(connection=conn, heartbeat_config=heartbeat_config)
```