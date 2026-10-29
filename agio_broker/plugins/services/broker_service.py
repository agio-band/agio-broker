import logging
import logging
import os
from queue import Queue, Empty
from threading import Thread
from urllib.parse import urlparse

from pydantic import BaseModel

from agio.core import settings
from agio.core.actions import make_action
from agio.core.events import emit
from agio.core.exceptions import ServiceStartupError
from agio.core.plugins.base_service import ThreadServicePlugin
from agio.tools import get_store, process_utils
from agio_broker.lib.router import router
from agio_broker.lib.server import BrokerServer

logger = logging.getLogger(__name__)


class BrokerService(ThreadServicePlugin):
    name = "broker"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = Queue()
        self.broker_server = None
        self.response_map = {}
        self.worker_thread = None
        self.port = settings.get_local_settings().get('agio_broker.port', 8877)

    def before_start(self):
        # check if broker already running
        store = get_store()
        pid = store.get('broker_pid')
        if pid and process_utils.process_exists(pid):
            raise ServiceStartupError('Broker service is already running')
        store.set('broker_pid', os.getpid())

    def execute(self, **kwargs):
        # os.environ['AGIO_IS_BROKER_SERVER'] = 'true'
        # start requests receiver
        self.worker_thread = Thread(target=self.sync_worker, name='broker_worker')
        self.worker_thread.start()
        # start async local server
        self.broker_server = BrokerServer(self.queue, self.response_map, '127.0.0.1', self.port)
        self.broker_server.start()

    def stop(self):
        # stop server
        self.broker_server.stop()
        # waiting main server thread stopped
        super().stop()
        # stop worker
        self.worker_thread.join()

    @make_action()
    def ping(self, *args, **kwargs):
        if kwargs.get('error'):
            raise Exception('Test Error')
        return {'result': 'pong'}

    def sync_worker(self):
        while not self.is_stopped():
            try:
                request = self.queue.get(timeout=0.2)
            except Empty:
                continue
            if not request:
                continue
            request_id = request['id']
            future = self.response_map.get(request_id)
            if future and not future.done():
                try:
                    request['path'] = urlparse(request['path']).path
                    result = self.process_request(request, request_id)
                    self.broker_server.loop.call_soon_threadsafe(future.set_result, result)
                except Exception as err:
                    emit('core.message.error', {'message': str(err)})
                    logger.error(str(err))
                    self.broker_server.loop.call_soon_threadsafe(future.set_exception, err)
            else:
                logger.error('Task future not created or already done. Executing skipped.')

    def _parse_path(self, path):
        return urlparse(path).path

    def process_request(self, request: dict, request_id: str) -> dict | None:
        function = request['path'].strip('/').split('/')[0]
        resp = router.execute_func(function, request, request_id)
        if isinstance(resp, BaseModel):
            resp = resp.model_dump(mode='json')
        return resp
