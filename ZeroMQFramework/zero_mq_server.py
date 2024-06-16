# import threading
# import signal
# from ZeroMQFramework import ZeroMQRouter, ZeroMQWorkerManager, ZeroMQProtocol
#
#
# class ZeroMQServer:
#     def __init__(self, frontend_port: int, backend_port: int, protocol: ZeroMQProtocol = ZeroMQProtocol.TCP, num_workers: int = 1):
#         self.frontend_port = frontend_port
#         self.backend_port = backend_port
#         self.protocol = protocol
#         self.num_workers = num_workers
#         self.router = ZeroMQRouter(frontend_port, protocol, backend_port, ZeroMQProtocol.IPC)
#         self.worker_manager = ZeroMQWorkerManager(backend_port, ZeroMQProtocol.IPC, num_workers)
#         self.running = True
#
#         signal.signal(signal.SIGINT, self.request_shutdown)
#         signal.signal(signal.SIGTERM, self.request_shutdown)
#
#     def request_shutdown(self, signum, frame):
#         print(f"Received signal {signum}, shutting down gracefully...")
#         self.running = False
#         self.router.request_shutdown(signum, frame)
#         self.worker_manager.request_shutdown(signum, frame)
#
#     def start(self):
#         # Start the router in a separate thread
#         router_thread = threading.Thread(target=self.router.start)
#         router_thread.start()
#         print(f"Router started on port {self.frontend_port}")
#
#         # Start the worker manager
#         self.worker_manager.start()
#         print(f"{self.num_workers} workers started on IPC port {self.backend_port}")
#
#         # Join the router thread to keep the main thread alive
#         router_thread.join()
