from agio.core.settings import APackageSettings, StringField


class BrokerSettings(APackageSettings):
    port: int = 8080

