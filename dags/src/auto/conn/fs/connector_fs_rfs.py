import filecmp
import hashlib
import os
import tempfile
import time
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from stat import S_ISDIR
from zipfile import ZipFile

import pandas as pd
import paramiko
from pyspark.sql.utils import AnalysisException

import src.auto.env as env
from src.auto.base.base_conn_fs import BaseConnFs

lfs_data_path = env.PRM_LFS_PATH + "/data"
lfs_tmp_path = env.PRM_LFS_PATH + "/tmp"


class ConnectorFsRfs(BaseConnFs):

    def __init__(self, object_prop: dict,
                 hostname=None, username=None, password=None, look_for_keys=False, allow_agent=False):
        super().__init__(object_prop)

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(
            hostname=hostname,
            username=username,
            password=password
        )
        # look_for_keys = look_for_keys,
        # allow_agent = allow_agent,
        self.obj_temp_dir = Path(lfs_data_path)  # tempfile.TemporaryDirectory()
        # self.temp_dir = self.obj_temp_dir.name
        pass

    def close(self):
        super().close()
        pass

    def __on_exit__(self):
        self.ssh_client.close()
        # self.obj_temp_dir.cleanup()

    def __close_temp_sftp(self, sftp, a_sftp):
        if a_sftp is None:
            sftp.close()
        else:
            # genel kullanımdakini kapamayacağız
            pass

    def __create_temp_sftp(self, a_sftp):
        if a_sftp is None:
            return self.ssh_client.open_sftp()

        return a_sftp

    def _check_save_mode(self, dataframe, kwargs):

        if "save_mode" in kwargs:
            if kwargs["save_mode"] not in [
                "append",
                "errorIfExists",
                "ignore",
                "overwrite",
            ]:
                raise Exception("unknown save_mode")

    def _rewrite_for_pandas(self, path, a_sftp=None, **kwargs):
        if "save_mode" in kwargs:

            if kwargs["save_mode"] == "overwrite":
                kwargs["mode"] = "w"
            elif kwargs["save_mode"] == "append":
                kwargs["mode"] = "a"
                if self.exists(path, a_sftp):
                    kwargs["header"] = None

            if self.exists(path, a_sftp):
                if kwargs["save_mode"] == "ignore":
                    pass
                elif kwargs["save_mode"] == "errorIfExists":
                    raise AnalysisException("dosya mevcut! ", None)

            del kwargs["save_mode"]

        return kwargs

    def get_permissions(self, path, a_sftp=None):
        """
        :return: Dosyanın izinlerini döner 3 rakamdan olusacak sekilde
        """
        sftp = self.__create_temp_sftp(a_sftp)
        stat = sftp.stat(path)
        self.__close_temp_sftp(sftp, a_sftp)

        chars = (str(stat))[1:10]
        res = []
        for i in range(0, 9, 3):
            sub = chars[i: i + 3]
            sub = (
                sub.replace("r", "1")
                .replace("w", "1")
                .replace("x", "1")
                .replace("-", "0")
            )
            v = int(sub, 2)
            res.append(str(v))
        result = "".join(res)
        return result

    def set_permissions(self, path, permission=0o777, a_sftp=None):
        if isinstance(permission, str):
            permission = int(permission, 8)
        sftp = self.__create_temp_sftp(a_sftp)
        sftp.chmod(path, permission)
        self.__close_temp_sftp(sftp, a_sftp)

    def compare(self, compare_level, path_1, path_2, a_sftp=None):
        """
        Iki dosyayi büyüklük ya da icerik acisindan karsilastirir
        :param path1:
        :param path2:
        :param compare_level: karsilastirma seviyesi, SIZE ya da CONTENT secilebilir
        :return: True ya da False doner
        """

        sftp = self.__create_temp_sftp(a_sftp)
        if compare_level == "SIZE":
            s1 = sftp.stat(path_1).st_size
            s2 = sftp.stat(path_2).st_size
            result = s1 == s2
        elif compare_level == "CONTENT":
            local_1 = self.download_to_tempdir(path_1, sftp)
            local_2 = self.download_to_tempdir(path_2, sftp)
            filecmp.clear_cache()
            result = filecmp.cmp(local_1, local_2)
        elif compare_level == "CHECK_SUM":
            result = self.get_hash_code(path_1, a_sftp=sftp) == self.get_hash_code(
                path_2, a_sftp=sftp
            )
        else:
            raise Exception(
                "Gecersiz bir karsilastirilmasi seviyesi girildi, SIZE ya da CONTENT secimi yapin !"
            )

        self.__close_temp_sftp(sftp, a_sftp)
        return result

    def create_temp_file(self, prefix, suffix, directory="/__os_temp__"):
        raise Exception("not implemented!")

    def create_temp_directory(self, a_sftp=None, prefix=None, suffix=None, directory=None):
        sftp = self.__create_temp_sftp(a_sftp)
        result = tempfile.mkdtemp()
        sftp.mkdir(result)
        self.__close_temp_sftp(sftp, a_sftp)
        return result

    def delete(self, path, recursive=False, skip_trash=True, a_sftp=None):
        """
        Dosya siler.
        :return:
        """
        if path in ["/"]:
            raise Exception("Kök dizin silinemez!")

        sftp = self.__create_temp_sftp(a_sftp)

        if recursive:
            cmd = f"rm -rf {path}"
            _stdin, stdout, _stderr = self.ssh_client.exec_command(cmd)
            while not stdout.channel.exit_status_ready():
                time.sleep(2)
        else:
            sftp.remove(path)

        self.__close_temp_sftp(sftp, a_sftp)

    def delete_on_exit(self, path):
        raise Exception("not implemented!")

    def exists(self, path, a_sftp=None):
        """
        dosya/dizin in var olup olmadığını sorgular
        :param path:
        :return: True yada False döner
        """
        try:
            sftp = self.__create_temp_sftp(a_sftp)
            sftp.stat(path)
            self.__close_temp_sftp(sftp, a_sftp)
            return True
        except Exception as e:
            return False

    def get_file_info(self, path, a_sftp=None):

        sftp = self.__create_temp_sftp(a_sftp)
        result = dict()
        stat = sftp.stat(path)

        result["is_file"] = str(stat)[0] == "-"
        result["size"] = stat.st_size
        result["st_size"] = stat.st_size
        self.__close_temp_sftp(sftp, a_sftp)

        return result

    def get_hash_code(self, path, hash_type="md5", block_size=65536, a_sftp=None):
        """
        Dosyaya ait hash kodunu doner
        :type: Hash tipini belirtir, default değeri md5 dir, ayrica sha1 olarak da hesaplanabilir
        :block_size: Dosya okuyacagi chunk buyukluklerini belirtir,
        dosya boyutuna gore guncellenebilir, default is 64KB
        :return: Dosyaya ait hash kodu
        """

        sftp = self.__create_temp_sftp(a_sftp)
        try:
            local_path = self.download_to_tempdir(path, sftp)
            if hash_type == "md5":
                hasher = hashlib.md5()
            elif hash_type == "sha1":
                hasher = hashlib.sha1()
            else:
                raise ValueError(
                    "unsupported  hash type, use md5 or sha1 !"
                )

            with open(local_path, "rb") as afile:
                buf = afile.read(block_size)

                while len(buf) > 0:
                    hasher.update(buf)
                    buf = afile.read(block_size)

            return hasher.hexdigest()

        except FileNotFoundError as err:
            print(err)
            raise Exception("Hash bilgisi alinacak dosya bulunamadi !")
        except Exception as e:
            print(e)
            raise Exception("Dosyanin hash kodunu bulurken hata ile karsilasildi!")

        finally:
            self.__close_temp_sftp(sftp, a_sftp)

    def list_file_names(self, pattern, a_sftp=None):
        sftp = self.__create_temp_sftp(a_sftp)
        result = sftp.listdir(pattern)

        result = list(map(lambda x: pattern + "/" + x, result))
        self.__close_temp_sftp(sftp, a_sftp)
        return result

    def list_file_info(self, pattern, a_sftp=None):
        result = []
        sftp_client = self.__create_temp_sftp(a_sftp)
        file_list = sftp_client.listdir_attr(lfs_data_path)

        for info in file_list:
            # print(info, " ", dir(info))
            inf = dict()
            inf["is_file"] = str(info)[0] == "-"
            inf["size"] = info.st_size
            inf["st_size"] = info.st_size
            inf["st_mode"] = info.st_mode
            inf["file_name"] = info.filename
            inf["path"] = lfs_data_path + "/" + info.filename

            result.append(inf)

        self.__close_temp_sftp(sftp_client, a_sftp)
        return result

    def rename(self, src_path, dest_path, a_sftp=None):
        """
        Dosyanın ismini günceller
        :param new_name: yeni dosya ismi
        :return:
        """
        try:
            sftp = self.__create_temp_sftp(a_sftp)
            sftp.rename(src_path, dest_path)
            self.__close_temp_sftp(sftp, a_sftp)
        except Exception as e:
            raise e

    def write_csv(self, path, dataframe, a_sftp=None, **kwargs):
        """
        dataframe içeriğini CSV olarak yazar. Pandas/Spark DF nesne tiplerini kendisi kontrol eder
        :param local_path: local path
        :param dataframe: pandas yada spark df
        :param kwargs: tamamlayıcı bileşenler
        """
        sftp = self.__create_temp_sftp(a_sftp)

        # varsa sil
        local_path = self.make_temp_path(path)
        if os.path.exists(local_path):
            os.remove(local_path)

        self._check_save_mode(dataframe, kwargs)

        # burasi onemli
        if "save_mode" in kwargs and kwargs["save_mode"] == "append":
            if self.exists(path, sftp):
                self.download_to_tempdir(path, sftp)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            if "header" in kwargs:
                kwargs["header"] = True
            result = dataframe.to_csv(local_path, **kwargs)
        else:
            raise Exception(f"unsupported  :{type(dataframe)}")

        self.upload_file(local_path, path, sftp)

        self.__close_temp_sftp(sftp, a_sftp)
        return result

    def write_excel(self, path, dataframe, **kwargs):
        """
        dataframe içeriğini Excel olarak yazar. Pandas/Spark DF nesne tiplerini kendisi kontrol eder
        :param local_path: local path
        :param dataframe: pandas yada spark df
        :param kwargs: tamamlayıcı bileşenler
        """
        local_path = self.make_temp_path(path)
        self._check_save_mode(dataframe, kwargs)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            kwargs["index"] = False
            result = dataframe.to_excel(local_path, **kwargs)
        else:
            raise Exception(f"unsupported :{type(dataframe)}")

        self.upload_file(local_path, path)

        return result

    def write_json(self, path, dataframe, orient="records", **kwargs):
        """
        dataframe içeriğini Json olarak yazar. Pandas/Spark DF nesne tiplerini kendisi kontrol eder
        :param local_path: local path
        :param dataframe: pandas yada spark df
        :param kwargs: tamamlayıcı bileşenler
        """

        local_path = self.make_temp_path(path)

        result = None
        self._check_save_mode(dataframe, kwargs)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            out = dataframe.to_json(orient=orient, **kwargs)
            with open(local_path, "wb") as jsn:  # pylint: disable=W1514
                jsn.write(out)
        else:
            raise Exception(f"unsupported :{type(dataframe)}")

        self.upload_file(local_path, path)

        return result

    def write_parquet(self, path, dataframe, **kwargs):
        """
        dataframe içeriğini Parquet olarak yazar.
        Pandas/Spark DF nesne tiplerini kendisi kontrol eder
        :param local_path: local path
        :param dataframe: pandas yada spark df
        :param kwargs: tamamlayıcı bileşenler
        """

        local_path = self.make_temp_path(path)

        result = None
        self._check_save_mode(dataframe, kwargs)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            dataframe.to_parquet(local_path, **kwargs)
        else:
            raise Exception(f"unsupported :{type(dataframe)}")

        self.upload_file(local_path, path)

        return result

    def read_csv(self, path, a_sftp=None, **kwargs):
        local_path = self.download_to_tempdir(path, a_sftp)
        kwargs["header"] = 0 if kwargs["header"] == True else None
        result = pd.read_csv(local_path, **kwargs)
        return result

    def read_excel(self, path, a_sftp=None, **kwargs):
        local_path = self.download_to_tempdir(path, a_sftp)
        result = pd.read_excel(local_path, **kwargs)
        if kwargs is not None and "astype" in kwargs:
            result = result.astype(kwargs["astype"])
        return result

    def read_json(self, path, orient="records", a_sftp=None, **kwargs):
        local_path = self.download_to_tempdir(path, a_sftp)
        result = pd.read_json(local_path, orient, **kwargs)
        return result

    def read_parquet(self, path, a_sftp=None, **kwargs):
        local_path = self.download_to_tempdir(path, a_sftp)
        result = pd.read_parquet(local_path, **kwargs)
        return result

    def mkdirs(self, path, permission=0o777, exist_ok=False, a_sftp=None):

        sftp = self.__create_temp_sftp(a_sftp)
        if isinstance(permission, str):
            permission = int(permission, 8)

        try:
            if not exist_ok:
                sftp.mkdir(path)
                sftp.chmod(path, permission)
            else:
                path_parts = path.split("/")
                _temp = ""
                for part in path_parts:
                    _temp += "/" + part
                    if self.exists(_temp, a_sftp=sftp):
                        pass
                    else:
                        sftp.mkdir(_temp)
                        sftp.chmod(_temp, permission)

            self.__close_temp_sftp(sftp, a_sftp)
            return True
        except Exception as e:
            return False

    def write(self, path, data, mode="wb", a_sftp=None):

        sftp = self.__create_temp_sftp(a_sftp)

        file = sftp.file(path, mode, -1)
        file.write(data)
        file.flush()
        self.__close_temp_sftp(sftp, a_sftp)

    def make_temp_path(self, path):
        local_path = lfs_tmp_path + "/" + path.split("/")[-1]
        return local_path

    def upload_file(self, local_path, path, a_sftp=None):
        sftp = self.__create_temp_sftp(a_sftp)
        sftp.put(local_path, path)
        self.__close_temp_sftp(sftp, a_sftp)

    def sftp_put_recursive(self, local, remote, sftp=None):
        if os.path.isfile(local):
            self.upload_file(local, remote, sftp)
            return

        for folder, _subfolders, filenames in os.walk(local):
            for filename in filenames:
                filepath = os.path.join(folder, filename).replace("\\", "/")

                relative_path = folder.replace("\\", "/").replace(
                    local.replace("\\", "/"), ""
                )
                remote_path = remote + relative_path
                remote_path = remote_path.replace("\\", "/")

                if not self.exists(remote_path):
                    self.mkdirs(remote_path)
                remote_path = os.path.join(remote_path, filename).replace("\\", "/")
                self.upload_file(filepath, remote_path, sftp)

    def sftp_get_recursive(self, path, dest, sftp):
        print(path, "==>", sftp.lstat(path))

        is_file = str(sftp.lstat(path))[0] == "-"

        if is_file:
            sftp.get(path, dest)
            print("sftp_get_recursive tmp", dest)
        else:
            item_list = sftp.listdir_attr(path)
            if not os.path.isdir(dest):
                os.makedirs(dest, exist_ok=True)
            for item in item_list:
                mode = item.st_mode
                _remote_path = path + "/" + item.filename
                _local_path = dest + "/" + item.filename
                if S_ISDIR(mode):
                    if not os.path.exists(_local_path):
                        os.makedirs(_local_path, 0o777, exist_ok=True)
                    self.sftp_get_recursive(_remote_path, _local_path, sftp)
                else:
                    print("bu alana girebildi ")
                    sftp.get(_remote_path, _local_path)

    def download_to_tempdir(self, path, a_sftp=None):
        sftp = self.__create_temp_sftp(a_sftp)
        local_path = self.make_temp_path(path)
        # sftp.get(path, local_path)
        self.sftp_get_recursive(path, local_path, sftp)
        self.__close_temp_sftp(sftp, a_sftp)
        return local_path

    def write_txt(self, path, data, mode="w", a_sftp=None):
        sftp = self.__create_temp_sftp(a_sftp)

        with sftp.open(path, mode) as txt_file:
            txt_file.write(data)

        self.__close_temp_sftp(sftp, a_sftp)

    def write_bin(self, path, data, mode="wb", a_sftp=None):
        sftp = self.__create_temp_sftp(a_sftp)
        # mod alternate (path, mode + "b")
        with sftp.open(path, mode) as bin_file:
            bin_file.write(data)

        self.__close_temp_sftp(sftp, a_sftp)

    def read_xml(self, path, **kwargs):
        _temp_path = self.download_to_tempdir(path)
        tree = ET.parse(_temp_path)
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
        result = None
        self._check_save_mode(dataframe, kwargs)

        # generic
        def write_to_xml(path, columns, data):

            local_path = self.make_temp_path(path)
            with open(local_path, "wb") as file:  # pylint: disable=W1514
                header = '<?xml version="1.0"?>\n<data>\n'
                file.write(header)
                for i, r in data:
                    row_str = f"<record id='{i}'>\n"
                    for c in columns:
                        row_str += f"\t<{c}>{r[c]}</{c}>\n"
                    row_str += "</record>\n"
                    file.write(row_str)
                end_data = "</data>"
                file.write(end_data)
                file.flush()
                file.close()

                self.upload_file(local_path=local_path, path=path)

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
        local_source = self.download_to_tempdir(source)
        local_dest = self.make_temp_path(str(uuid.uuid4()) + ".zip").replace("\\", "/")
        with ZipFile(local_dest, "w") as zip:
            if os.path.isdir(local_source):
                for folder, _subfolders, filenames in os.walk(local_source):
                    for filename in filenames:
                        filepath = os.path.join(folder, filename).replace("\\", "/")
                        if filepath == local_dest:
                            continue

                        relative_path = folder.replace("\\", "/").replace(
                            local_source.replace("\\", "/"), ""
                        )
                        zip_path = os.path.join(relative_path, filename)
                        zip.write(filepath, zip_path)
            else:
                zip.write(local_source, os.path.basename(local_source))
        self.upload_file(local_path=local_dest, path=dest)

    def unzip(self, source, dest, print_dir=True):

        local_source = self.make_temp_path("/" + source.replace("\\", "/").split("/")[-1])
        sftp = self.__create_temp_sftp(a_sftp=None)
        sftp.get(source, local_source)
        local_dest = self.make_temp_path("/" + str(uuid.uuid4()))
        with ZipFile(local_source, "r") as zip:
            if print_dir:
                zip.printdir()
            zip.extractall(path=local_dest)

        self.sftp_put_recursive(local_dest, dest)

    def copy(self, source, dest):
        sftp = self.__create_temp_sftp(a_sftp=None)
        local_temp = self.make_temp_path("") + str(uuid.uuid4())
        self.sftp_get_recursive(source, local_temp, sftp)
        self.sftp_put_recursive(local_temp, dest, sftp)

    def move(self, source, dest, a_sftp=None):

        """
               Dosya tasima islemini yapar
               :param source: dosya path'i
               :param dest:taşınmak istenilen path.dosya daha önce bu path de bulunmamalı.
               :return:
               """
        try:
            sftp = self.__create_temp_sftp(a_sftp)

            sftp.rename(source, dest)

            self.__close_temp_sftp(sftp, a_sftp)
        except Exception as e:
            raise e

    def read_txt(self, path, a_sftp=None):

        local_path = self.download_to_tempdir(path, a_sftp)

        result = ""
        with open(local_path, "r", encoding="utf-8") as txt:
            result = txt.read()

        return result

    def read_binary(self, path, a_sftp=None):

        local_path = self.download_to_tempdir(path, a_sftp)

        result = ""
        with open(local_path, "rb") as binary:
            result = binary.read()
        return result
