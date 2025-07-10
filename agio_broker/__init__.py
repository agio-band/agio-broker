import requests


def send_action(name, *args, **kwargs):
    data = dict(
        action=name,
        args=args,
        kwargs=kwargs,
    )
    return requests.post('http://localhost:8080/action', json=data).json()