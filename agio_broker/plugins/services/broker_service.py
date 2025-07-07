import asyncio
import time
from queue import Queue

from agio.core.plugins.base.service_base import ServicePlugin, action
from agio_broker.lib.server import BrokerServer


class BrokerService(ServicePlugin):
    name = "broker"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = Queue()
        self.broker_server = None
        self.response_map = {}

    def execute(self, **kwargs):
        self.broker_server = BrokerServer(self.queue, self.response_map, '127.0.0.1', 8080)
        self.broker_server.start()

    def stop(self):
        self.broker_server.stop()
        return super().stop()

    def sync_worker(self):
        while not self.is_stopped():
            task = self.queue.get(timeout=0.2)
            if not task:
                continue
            request_id = task['id']
            try:
                # Пример обработки
                response = {
                    "echo": task,
                    "message": "Processed in thread"
                }
                # time.sleep(0.2)
            except Exception as e:
                response = {"error": str(e)}

            future = self.response_map.get(request_id)
            if future and not future.done():
                self.broker_server.loop.call_soon_threadsafe(future.set_result, response)
