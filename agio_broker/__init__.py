import requests
from agio.core import settings


def send_action(name, *args, **kwargs):
    data = dict(
        action=name,
        args=args,
        kwargs=kwargs,
    )
    s = settings.get_local_settings()
    port = s.get('agio_broker.port')
    return requests.post(f'http://localhost:{port}/action', json=data).json()