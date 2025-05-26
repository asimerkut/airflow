from abc import abstractmethod

from src.auto.base.base_conn import BaseConn


class BaseConnFs(BaseConn):

    def __init__(self, object_prop: dict):
        super().__init__(object_prop)
        pass

    @abstractmethod
    def close(self):
        print("Close Fs:"+str(self.object_prop))
        pass

    @abstractmethod
    def __on_exit__(self):
        pass

    @abstractmethod
    def get_permissions(self, path):
        pass

    @abstractmethod
    def set_permissions(self, path, mode):
        pass

    @abstractmethod
    def compare(self, compare_level, path_1, path_2):
        pass

    @abstractmethod
    def create_temp_file(self, prefix, suffix, directory="/__os_temp__"):
        pass

    @abstractmethod
    def delete(self, path, recursive=False, skip_trash=True):
        pass

    @abstractmethod
    def delete_on_exit(self, path):
        pass

    @abstractmethod
    def exists(self, path):
        pass

    @abstractmethod
    def get_file_info(self, path):
        pass

    @abstractmethod
    def get_hash_code(self, path):
        pass

    @abstractmethod
    def list_file_names(self, pattern):
        pass

    @abstractmethod
    def list_file_info(self, pattern):
        pass

    @abstractmethod
    def rename(self, src_path, dest_path):
        pass

    @abstractmethod
    def write_csv(self, path, dataframe, **kwargs):
        pass

    @abstractmethod
    def write_excel(self, path, dataframe, **kwargs):
        pass

    @abstractmethod
    def write_json(self, path, dataframe, **kwargs):
        pass

    @abstractmethod
    def write_parquet(self, path, dataframe, **kwargs):
        pass

    @abstractmethod
    def read_csv(self, path, **kwargs):
        pass

    @abstractmethod
    def read_excel(self, path, **kwargs):
        pass

    @abstractmethod
    def read_json(self, path, **kwargs):
        pass

    @abstractmethod
    def read_parquet(self, path, **kwargs):
        pass

    @abstractmethod
    def mkdirs(self, path, permission="777"):
        pass

    @abstractmethod
    def read_xml(self, path, **kwargs):
        pass

    @abstractmethod
    def write_xml(self, path, dataframe, **kwargs):
        pass

    @abstractmethod
    def zip(self, source, dest):
        pass

    @abstractmethod
    def unzip(self, source, dest):
        pass

    @abstractmethod
    def copy(self, source, dest):
        pass
