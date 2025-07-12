import requests
from agio.core.settings import get_local_settings


def send_action(name, *args, **kwargs):
    data = dict(
        action=name,
        args=args,
        kwargs=kwargs,
    )
    settings = get_local_settings()
    port = settings.get('agio_broker.port')
    return requests.post(f'http://localhost:{port}/action', json=data).json()