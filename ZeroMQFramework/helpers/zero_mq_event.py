from enum import Enum


class ZeroMQEvent(Enum):
    REGISTER_ROUTER = "register_router"
    REGISTER_WORKER = "register_worker"
    HEARTBEAT = "heartbeat"
    RESPONSE = "response"
    GET_ROUTER_FOR_CLIENT = "get_router_for_client"
    GET_ROUTER_FOR_WORKER = "get_router_for_worker"
