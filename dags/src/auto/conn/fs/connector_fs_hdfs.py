import atexit
import io
import re
import xml.etree.ElementTree as ET

import pandas as pd
from hdfs import InsecureClient

from src.auto.base.base_conn_fs import BaseConnFs


# r"""
# https://hdfscli.readthedocs.io/en/latest/api.html
# https://www.journaldev.com/19178/python-io-bytesio-stringio
# """


class ConnectorFsHdfs(BaseConnFs):

    def __init__(self, object_prop: dict,
                 url=None, user=None, spark_hdfs=None):
        super().__init__(object_prop)

        # url='http://10.6.170.51:9870', user='appadmin', spark_hdfs='hdfs://10.6.170.165:9000'
        self.__version__ = "21.9.15"
        self._client_hdfs = InsecureClient(url=url, user=user)
        self._spark_hdfs = spark_hdfs
        self._delete_files_on_exit_ = []
        atexit.register(self.__on_exit__)
        self.error_list = [
            "/",
            "/app",
            "/app/lfs",
            "/home",
            "/tmp",
            "/user",
        ]

    def close(self):
        super().close()
        pass

    def __on_exit__(self):
        for file in self._delete_files_on_exit_:
            try:
                self._client_hdfs.delete(file)
            except Exception as e:
                print(e)
                raise e

    def _check_save_mode(self, dataframe, kwargs):

        if "save_mode" in kwargs:
            if kwargs["save_mode"] not in [
                "append",
                "errorIfExists",
                "ignore",
                "overwrite",
            ]:
                raise Exception("unknown save_mode")

    def _set_save_mode(self, write, kwargs):
        if "save_mode" in kwargs:
            write.mode(kwargs["save_mode"])

    def _set_options(self, _prepare, kwargs):

        if "header" in kwargs and kwargs["header"]:
            kwargs["header"] = "true"

        for k in kwargs:
            _prepare = _prepare.option(k, kwargs[k])

        return _prepare

    def get_permissions(self, path):
        """
        belirtilen path için izinleri okur
        :param path: HDFS yolu
        """
        try:
            return self._client_hdfs.status(path)["permission"]
        except Exception as e:
            raise Exception(e)

    def set_permissions(self, path, mode):
        """
        belirtilen path için mode ataması yapar
        :param path: hdfs path
        :param mode: mode
        """
        self._client_hdfs.set_permission(path, mode)

    def compare(self, compare_level, path_1, path_2):
        """
        iki dosyayı kıyaslar,
        :param compare_level: CHECK_SUM, CONTENT, SIZE değerlerinden birini alır
        :param path_1: hdfs path
        :param path_2: hdfs path

        True ya da False döner
        """
        if compare_level == "CHECK_SUM":
            result = self._client_hdfs.checksum(path_1) == self._client_hdfs.checksum(
                path_2
            )
        elif compare_level == "CONTENT":
            result = self._client_hdfs.content(path_1) == self._client_hdfs.content(
                path_2
            )
        elif compare_level == "SIZE":
            result = (
                    self._client_hdfs.status(path_1)["length"]
                    == self._client_hdfs.status(path_2)["length"]
            )
        else:
            raise Exception("unknown nitelik: " + compare_level)
        return result

    def create_temp_file(self, prefix, suffix, directory="/__os_temp__"):
        """
        geçici dosya oluşturur
        :param prefix:
        :param suffix:
        :param directory:
        :return: dosya tipi
        """
        raise Exception("not implemented!")

    def delete(self, path, recursive=False, skip_trash=True):
        """
        dosya dizin siler. boş olmayan dizinler için recursive parametresi True verilmeli
        :param path: hdfs path
        :param recursive: boş olmayan dizin ise True atanmalı
        :param skip_trash: kazara silinmeyi onlemek için
        """

        if path in self.error_list:
            raise Exception("bu dizin silinemez!")

        return self._client_hdfs.delete(path, recursive=recursive, skip_trash=skip_trash)

    def delete_on_exit(self, path):
        """
        uygulamadan çıkılırken (program hatasız sonlanmışsa) ilgili path i silmek üzere kaydeder
        :param path: hdfs path
        """
        self._delete_files_on_exit_.append(path)

    def exists(self, path):
        """
        dosya dizin varlığını kontrol eder
        :param path: hdfs path

        True ya da False doner
        """
        result = True
        try:
            self._client_hdfs.status(path)
        except Exception as e:
            result = False

        return result

    def get_file_info(self, path):
        """
        bir dosya ve dizin için bilgi doner
        :param path: hdfs path
        Json doner
        """
        return self._client_hdfs.status(path)

    def get_hash_code(self, path):
        """
        bir dosya için checksum değerini doner
        :param path: hdfs path
        json nesnesi doner
        """
        return self._client_hdfs.checksum(path)

    def list_file_names(self, path, pattern=None):
        """
        Belirli bir Path ve pattern için uyan dosya/dizin bilgisini doner
        :param pattern: hdfs path
        dosya isimlerinden oluşan bir dizi doner
        """
        result = self._client_hdfs.list(path)
        if pattern is not None:
            pattern = pattern.replace('.', '\\.').replace('*', '.*')

            result = [item for item in result if re.search(pattern, item)]

        return result

    def list_file_info(self, path, pattern):
        """
        verilen bir path ve pattern için status değerlerini doner
        :param pattern: hdfs path
        fs info json dizisi doner
        """
        result = self._client_hdfs.list(path)

        if pattern is not None:
            pattern = pattern.replace('.', '\\.').replace('*', '.*')

            result = [item for item in result if re.search(pattern, item[0])]

        return result

    def rename(self, src_path, dest_path):
        """
        dosya/dizin için ad değiştirir veya taşır
        :param src_path: kaynak
        :param dest_path: hedef
        """

        if src_path in self.error_list:
            raise Exception("exc !", src_path)

        if dest_path in self.error_list:
            raise Exception("exc !", dest_path)

        self._client_hdfs.rename(src_path, dest_path)

    def write_csv(self, path, dataframe, encoding="utf-8", **kwargs):
        """
        dataframe içeriğini CSV olarak yazar. Pandas/Spark DF nesne tiplerini kendisi kontrol eder
        :param path: hdfs path
        :param dataframe: pandas yada spark df
        :param kwargs: tamamlayıcı bileşenler
        """
        result = None
        self._check_save_mode(dataframe, kwargs)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            with self.engine().write(path, encoding=encoding) as writer:
                dataframe.to_csv(writer, **kwargs)
        else:
            raise Exception(f"unsupported :{type(dataframe)}")

        return result

    def write_excel(self, path, dataframe, **kwargs):
        """
        dataframe içeriğini excel olarak yazar. Pandas/Spark DF nesne tiplerini kendisi kontrol eder
        :param path:
        :param dataframe:
        :param kwargs:
        :return:
        """
        result = None
        self._check_save_mode(dataframe, kwargs)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            with self.engine().write(path, ) as writer:
                dataframe.to_excel(writer, **kwargs)
        else:
            raise Exception(f"unsupported :{type(dataframe)}")

        return result

    def write_json(self, path, dataframe, orient="records", **kwargs):
        """
        dataframe içeriğini JSON olarak yazar. Pandas/Spark DF nesne tiplerini kendisi kontrol eder
        :param path:
        :param dataframe:
        :param kwargs:
        :return:
        """
        result = None
        self._check_save_mode(dataframe, kwargs)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            # https://datatofish.com/export-pandas-dataframe-json/
            with self.engine().write(path, encoding="utf-8") as writer:
                # writer.write(line)
                dataframe.to_json(writer, orient=orient, **kwargs)
        else:
            raise Exception(f"unsupported :{type(dataframe)}")

        return result

    def write_parquet(self, path, dataframe, **kwargs):
        """
        dataframe içeriğini Parquet olarak yazar.
        Pandas/Spark DF nesne tiplerini kendisi kontrol eder
        :param path:
        :param dataframe:
        :param kwargs:
        :return:
        """
        result = None
        self._check_save_mode(dataframe, kwargs)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            file = io.BytesIO()
            dataframe.to_parquet(file)
            with self.engine().write(path) as writer:
                writer.write(file.read())
        else:
            raise Exception(f"unsupported :{type(dataframe)}")

        return result

    # OKU
    def read_csv(self, path, **kwargs):

        if kwargs is not None and "sep" in kwargs:
            sep = kwargs["sep"]
            del kwargs["sep"]
            kwargs["delimiter"] = sep

        result = None
        with self.read(path) as reader:
            result = pd.read_csv(reader, **kwargs)
        return result

    def read_excel(self, path, **kwargs):
        with self.engine().read(path) as reader:
            stream_bytes = io.BytesIO(reader.read())
            result = pd.read_excel(stream_bytes, **kwargs)

            if kwargs is not None and "astype" in kwargs:
                result = result.astype(kwargs["astype"])

        return result

    def read_json(self, path, orient="records", **kwargs):
        with self.engine().read(path) as reader:
            content = reader.read()
            result = pd.read_json(content, orient, **kwargs)

        return result

    def read_parquet(self, path, **kwargs):
        """
        https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html
        :param path:
        :param kwargs:
        :return:
        """
        with self.engine().read(path) as reader:
            byte_stream = io.BytesIO(reader.read())
            result = pd.read_parquet(byte_stream, **kwargs)
        return result

    def read(
            self,
            hdfs_path,
            offset=0,
            length=None,
            buffer_size=None,
            encoding=None,
            chunk_size=0,
            delimiter=None,
            progress=None,
    ):
        """
        # https://hdfscli.readthedocs.io/en/latest/api.html
        :param hdfs_path:
        :param offset:
        :param length:
        :param buffer_size:
        :param encoding:
        :param chunk_size:
        :param delimiter:
        :param progress:
        :return:

        with client.read('foo') as reader:
            content = reader.read()
        """
        return self._client_hdfs.read(
            hdfs_path=hdfs_path,
            offset=offset,
            length=length,
            buffer_size=buffer_size,
            encoding=encoding,
            chunk_size=chunk_size,
            delimiter=delimiter,
            progress=progress,
        )

    def write(
            self,
            hdfs_path,
            data=None,
            overwrite=False,
            permission=None,
            blocksize=None,
            replication=None,
            buffersize=None,
            append=False,
            encoding=None,
    ):
        """

        :param hdfs_path:
        :param data:
        :param overwrite:
        :param permission:
        :param blocksize:
        :param replication:
        :param buffersize:
        :param append:
        :param encoding:
        :return:

        from json import dump, dumps

        records = [
          {'name': 'foo', 'weight': 1},
          {'name': 'bar', 'weight': 2},
        ]

        # As a context manager:
        with client.write('data/records.jsonl', encoding='utf-8') as writer:
          dump(records, writer)

        # Or, passing in a generator directly:
        client.write('data/records.jsonl', data=dumps(records), encoding='utf-8')
        """
        self._client_hdfs.write(
            hdfs_path,
            data,
            overwrite,
            permission,
            blocksize,
            replication,
            buffersize,
            append,
            encoding,
        )

    def upload(
            self,
            hdfs_path,
            local_path,
            n_threads=1,
            temp_dir=None,
            chunk_size=65536,
            progress=None,
            cleanup=True,
            **kwargs,
    ):
        self._client_hdfs.upload(
            hdfs_path,
            local_path,
            n_threads,
            temp_dir,
            chunk_size,
            progress,
            cleanup,
            **kwargs,
        )

    def mkdirs(self, path, permission="777", exist_ok=False):
        self._client_hdfs.makedirs(path, permission)

    def engine(self):
        return self._client_hdfs

    def read_xml(self, path, **kwargs):
        with self.read(path, encoding="utf-8") as reader:
            tree = ET.parse(reader)
            list_of_map = []
            data = tree.getroot()
            for element in data:
                row = {}
                for col in element:
                    row[col.tag] = col.text
                list_of_map.append(row)
            result = pd.DataFrame(list_of_map)
            return result

    def write_xml(self, path, dataframe, **kwargs):
        def write_to_xml(path, columns, data):
            with self.engine().write(path, encoding="utf-8") as writer:
                # writer.write(line)
                header = '<?xml version="1.0"?>\n<data>\n'
                writer.write(header)
                for i, r in data:
                    row_str = f"<record id='{i}'>\n"
                    for c in columns:
                        row_str += f"\t<{c}>{r[c]}</{c}>\n"
                    row_str += "</record>\n"
                    writer.write(row_str)
                end_data = "</data>"
                writer.write(end_data)
                writer.flush()

        result = None
        self._check_save_mode(dataframe, kwargs)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            columns = dataframe.columns.tolist()
            data = dataframe.iterrows()
            write_to_xml(path, columns, data)
        else:
            raise Exception(f"unsupported :{type(dataframe)}")

        return result

    def zip(self, source, dest):
        raise Exception("HDFS için geçersiz işlem!")

    def unzip(self, source, dest):
        raise Exception("HDFS için geçersiz işlemmmm!")

    def copy(self, source, dest):
        raise Exception("HDFS için geçersiz işlem!")

    def test(self):
        raise Exception("HDFS için geçersiz işlem!")

    def write_bin(self, path, data):
        with self.engine().write(path, encoding=None, overwrite=True) as writer:
            writer.write(data)

    def write_txt(self, path, data):
        with self.engine().write(path, encoding="utf-8") as writer:
            writer.write(data)

    def read_txt(self, path):
        with self.engine().read(path, encoding="utf-8") as reader:
            data = reader.read()
        return data

    def read_binary(self, path):
        with self.engine().read(path, encoding=None) as reader:
            data = reader.read()
        return data

    def move(self, source, dest):
        if not self.engine().content(source):
            raise Exception(f"{source} dosya veya dizini bulunamadı.")

        self.engine().rename(source, dest)

    def create_temp_directory(self, prefix=None, suffix=None, directory=None):
        raise Exception("HDFS için geçersiz işlem!")
