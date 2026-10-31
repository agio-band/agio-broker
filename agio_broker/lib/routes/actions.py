import json
import logging

from agio.core.entities.project import AProject
from agio.core.events import subscribe_manager
from agio.core.workspaces import AWorkspaceManager
from agio.tools import args_helper, get_store, launching, process_utils
from agio.core import actions
from agio_broker.lib.router import router

logger = logging.getLogger(__name__)

@router.add('action')
def execute_action(request: dict, request_id: str) -> dict | None:
    action_data = request['data']
    logger.debug('Action requested: %s', action_data)
    keep_console = bool(action_data.get('keep_console'))
    ws_id = get_workspace_id(action_data)
    if ws_id:
        # execute action as command with different workspace using project id
        workspace_manager = AWorkspaceManager.create_from_id(ws_id)
        logger.info(f'Executing action with workspace "{ws_id}"')
        args = args_helper.dict_to_args(action_data['kwargs'])
        cmd = [
            'action', action_data['action'],
            *args
        ]
        logger.info(f'Launch cmd (console={keep_console}): agio {" ".join(cmd)}')
        logger.info(f'WS ID: {workspace_manager.launching_id}')
        envs = {'AGIO_ACTION_REQUEST_ID': request_id}
        with subscribe_manager(
                'core.workspace.before_install',
                lambda payload: actions.execute_action(
                    'desk.show_message',
                    text="Workspace creation is started. Please wait...")):
            result = launching.exec_agio_command(
                cmd,
                envs=envs,
                workspace=workspace_manager.launching_id,
                use_custom_pipe=not keep_console,
                new_console=keep_console,
                keep_console=keep_console
            )
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
        return resp


def get_workspace_id(action_data: dict) -> str | None:
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

