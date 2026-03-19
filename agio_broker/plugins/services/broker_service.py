import json
import logging
import os
from queue import Queue, Empty
from threading import Thread

from pydantic import BaseModel

from agio.core import settings
from agio.core import actions
from agio.core.entities import AWorkspace
from agio.core.entities.project import AProject
from agio.core.events import emit, subscribe_manager
from agio.core.exceptions import ServiceStartupError
from agio.core.plugins.base_service import make_action, ThreadServicePlugin
from agio.core.workspaces import AWorkspaceManager
from agio.tools import args_helper, get_store, launching, process_utils
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
        os.environ['AGIO_IS_BROKER_SERVER'] = 'true'
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
        from agio.core import api
        while not self.is_stopped():
            try:
                task = self.queue.get(timeout=0.2)
            except Empty:
                continue
            if not task:
                continue
            request_id = task['id']
            future = self.response_map.get(request_id)
            if future and not future.done():
                try:
                    result = self.process_request(task, request_id)
                    self.broker_server.loop.call_soon_threadsafe(future.set_result, result)
                except Exception as err:
                    emit('core.message.error', {'message': str(err)})
                    # logger.exception('Failed to execute action function')
                    logger.error(str(err))
                    self.broker_server.loop.call_soon_threadsafe(future.set_exception, err)
            else:
                logger.error('Task future not created or already done. Executing skipped.')

    def process_request(self, request: dict, request_id: str) -> dict | None:
        """
        Supported functions:
        - action: execute remote action
        """
        function = request['path'].strip('/').split('/')[0]
        match function:
            case 'action':
                # execute remote action
                return self.execute_action(request, request_id)
            case _:
                raise Exception('Function not found: {function}')


    def execute_action(self, request: dict, request_id: str) -> dict | None:
        action_data = request['data']
        ws_id = self.get_workspace_id(action_data)
        if ws_id:
            # execute action as command with different workspace using project id
            workspace_manager = AWorkspaceManager.create_from_id(ws_id)
            logger.info(f'Executing action with workspace {ws_id}')
            args = args_helper.dict_to_args(action_data['kwargs'])
            cmd = [
                'action', action_data['action'],
                *args
            ]
            logger.info(f'Launch CMD: {" ".join(cmd)} with WS ID: {workspace_manager.launching_id}' )
            envs = {'AGIO_ACTION_REQUEST_ID': request_id}
            with subscribe_manager(
                    'core.workspace.before_install',
                    lambda payload: actions.execute_action('desk.show_message', text="Workspace creation is started. Please wait...")):
                result = launching.exec_agio_command(
                    cmd,
                    envs=envs,
                    workspace=workspace_manager.launching_id,
                    use_custom_pipe=True)
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
            args = action_data.get('args') or []
            kwargs = action_data.get('kwargs') or {}
            resp = action_func(*args, **kwargs)
            if isinstance(resp, BaseModel):
                resp = resp.model_dump(mode='json')
            return resp

    def get_workspace_id(self, action_data: dict) -> str|None:
        ws_id = action_data.get('workspace_id')
        if ws_id:
            return ws_id
        project_id = action_data.get('project_id')
        if project_id:
            project = AProject(project_id)
            ws_id = project.workspace_launching_id
            if not ws_id:
                raise ValueError(f'Workspace not configured for project {project.name}')
            return ws_id
        return None