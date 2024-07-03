from enum import Enum


class ZeroMQNodeType(Enum):
    ROUTER = "router"
    WORKER = "worker"
    CLIENT = "client"
    SERVER = "server"
    UNDEFINED = "undefined"
