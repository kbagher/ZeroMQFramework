from enum import Enum


class ZeroMQNodeType(Enum):
    ROUTER = "router"
    WORKER = "worker"
    CLIENT = "client"
    SERVER = "server"
    SERVICE_DISCOVERY = "service_discovery"
    UNDEFINED = "undefined"