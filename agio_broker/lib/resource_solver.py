from pathlib import Path

from agio.core.workspaces import AWorkspaceManager, APackageManager
from agio.tools.local_dirs import default_env_install_dir


def get_resource_by_path(rel_path, workspace_id=None):
    rel_path = rel_path.strip('/')
    for res_dir in collect_workspace_resource_dirs(workspace_id):
        path = Path(res_dir, rel_path)
        if path.is_file():
            return path
    raise FileNotFoundError(f"Resource {rel_path} not found")


def collect_workspace_resource_dirs(workspace_id=None):
    if workspace_id is None:
        root = default_env_install_dir()
        ws = AWorkspaceManager(root=root)
    else:
        ws = AWorkspaceManager.create_from_id(workspace_id)
    if not ws.root.exists():
        raise FileNotFoundError(f"Workspace {workspace_id} not installed")
    for pkg in ws.iter_installed_packages():
        pkg: APackageManager
        yield pkg.get_resource_dir()
