from abc import ABC, abstractmethod


class ConnectorDbAbc(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass
