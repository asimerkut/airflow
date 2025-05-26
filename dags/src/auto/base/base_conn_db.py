from abc import abstractmethod

from src.auto.base.base_conn import BaseConn


class BaseConnDb(BaseConn):

    def __init__(self, object_prop: dict):
        super().__init__(object_prop)
        pass

    @abstractmethod
    def close(self):
        print("Close Db:"+str(self.object_prop))
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def rollback(self):
        pass
