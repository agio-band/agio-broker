import time

from agio.core.plugins.base.service_base import AServicePlugin


class BrokerService(AServicePlugin):
    name = "broker"

    def execute(self):
        while not self.is_stopped():
            time.sleep(1)
