from abc import ABC, abstractmethod


class BaseConn(ABC):

    def __init__(self, object_prop: dict):
        self.object_prop = object_prop
        pass

    @abstractmethod
    def close(self):
        pass
