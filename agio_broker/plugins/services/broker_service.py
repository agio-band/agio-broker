import json
import logging
import os
import traceback
from queue import Queue, Empty
from threading import Thread
import click

from agio.core.entities.project import AProject
from agio.core.utils import store, actions
from agio.core.exceptions import ServiceStartupError
from agio.core.plugins.base_service import make_action, ThreadServicePlugin
from agio.core.utils.launch_utils import exec_agio_command
from agio.core.utils.process_utils import process_exists
from agio.core.utils import args_helper
from agio.core import settings
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

    def before_start(self):
        # check if broker already running
        pid = store.get('broker_pid')
        if pid and process_exists(pid):
            raise ServiceStartupError('Broker service is already running')
        store.set('broker_pid', os.getpid())

    def execute(self, **kwargs):
        s = settings.get_local_settings()
        # start requests receiver
        self.worker_thread = Thread(target=self.sync_worker, name='broker_worker')
        self.worker_thread.start()
        # start async local server
        self.broker_server = BrokerServer(self.queue, self.response_map, '127.0.0.1', s.get('agio_broker.port', 8877))
        self.broker_server.start()

    def stop(self):
        # stop server
        self.broker_server.stop()
        # waiting main server thread stopped
        super().stop()
        # stop worker
        self.worker_thread.join()

    @make_action()
    def ping(self):
        return {'result': 'pong'}

    def sync_worker(self):
        while not self.is_stopped():
            try:
                task = self.queue.get(timeout=0.2)
            except Empty:
                continue
            if not task:
                continue
            request_id = task['id']
            try:
                response = self.process_request(task)
            except Exception as e:
                response = {"error": str(e)}
                traceback.print_exc()

            future = self.response_map.get(request_id)
            if future and not future.done():
                self.broker_server.loop.call_soon_threadsafe(future.set_result, response)

    def process_request(self, request: dict) -> dict | None:
        function = request['path'].strip('/').split('/')[0]
        match function:
            case 'action':
                return self.execute_action(request)
            case _:
                raise Exception('Unknown request')

    def execute_action(self, request: dict) -> dict | None:
        action_data = request['data']
        project_id = action_data.get('project_id')
        if project_id:
            # execute action as command with different workspace
            project = AProject(project_id)
            workspace = project.get_workspace()
            if not workspace:
                raise Exception(f'Project {project.name} has no workspace')
            logger.debug(f'Executing action '
                         f'with project {project.code} ({project_id}) in workspace {workspace.name} ({workspace.id})')
            args = args_helper.dict_to_args(action_data['kwargs'])
            cmd = [
                'action', action_data['action'],
                *args
            ]
            logger.info(f'Launch CMD: {" ".join(cmd)}' )
            result = exec_agio_command(cmd, workspace=workspace.id, use_custom_pipe=True)
            try:
                return json.loads(result)
            except (json.decoder.JSONDecodeError, TypeError):
                return result
        else:
            action_name_full = action_data.get('action')
            logger.debug('Executing action %s', action_name_full)
            if not action_name_full:
                raise Exception('Action name not set')
            action_func = actions.get_action_func(action_name_full)
            args = action_data.get('args', [])
            kwargs = action_data.get('kwargs', {})
            try:
                return action_func(*args, **kwargs)
            except Exception as e:
                traceback.print_exc()

                from agio.tools import qt

                qt.show_message_dialog(str(e), 'Error', 'error')  # todo: replace with emit event
                return {'error': str(e)}
