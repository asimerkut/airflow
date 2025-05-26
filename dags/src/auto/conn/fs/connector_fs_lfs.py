import filecmp
import glob
import hashlib
import os
import shutil
import sys
import tempfile
import xml.etree.ElementTree as ET
from zipfile import ZipFile

import pandas as pd
from pyspark.sql.utils import AnalysisException

from src.auto.base.base_conn_fs import BaseConnFs


class ConnectorFsLfs(BaseConnFs):

    def __init__(self, object_prop: dict):
        super().__init__(object_prop)
        pass

    def close(self):
        super().close()
        pass

    def __on_exit__(self):
        pass

    def _check_save_mode(self, dataframe, kwargs):

        if "save_mode" in kwargs:
            if kwargs["save_mode"] not in [
                "append",
                "errorIfExists",
                "ignore",
                "overwrite",
            ]:
                raise Exception("unknown save_mode")

    def _rewrite_for_pandas(self, path, **kwargs):
        if "save_mode" in kwargs:

            if kwargs["save_mode"] == "overwrite":
                kwargs["mode"] = "w"
            elif kwargs["save_mode"] == "append":
                kwargs["mode"] = "a"
                if os.path.exists(path):
                    kwargs["header"] = None

            if os.path.exists(path):
                if kwargs["save_mode"] == "ignore":
                    pass
                elif kwargs["save_mode"] == "errorIfExists":
                    raise AnalysisException("dosya mevcut! ", None)

            del kwargs["save_mode"]

        return kwargs

    def get_permissions(self, path):
        """
        :return: Dosyanın izinlerini döner 3 rakamdan olusacak sekilde
        """
        try:
            st = os.stat(path)
            oct_perm = oct(st.st_mode)
            permission = oct_perm[len(oct_perm) - 3: len(oct_perm)]
            return permission
        except Exception as e:
            raise e

    def set_permissions(self, path, mode=0o777):
        if isinstance(mode, str):
            mode = int(mode, 8)
        os.chmod(path, mode)

    def compare(self, compare_level, path_1, path_2):
        """
        Iki dosyayi büyüklük ya da icerik acisindan karsilastirir
        :param path1:
        :param path2:
        :param compare_level: karsilastirma seviyesi, SIZE ya da CONTENT secilebilir
        :return: True ya da False doner
        """

        if compare_level == "SIZE":
            s1 = os.path.getsize(path_1)
            s2 = os.path.getsize(path_2)
            result = s1 == s2
        elif compare_level == "CONTENT":
            filecmp.clear_cache()
            result = filecmp.cmp(path_1, path_2)
        elif compare_level == "CHECK_SUM":
            result = self.get_hash_code(path_1) == self.get_hash_code(path_2)
        else:
            raise Exception(
                "Gecersiz bir karsilastirilmasi seviyesi girildi, SIZE ya da CONTENT secimi yapin !"
            )

        return result

    def create_temp_file(self, prefix, suffix, directory="/__os_temp__"):
        raise Exception("not implemented!")

    def create_temp_directory(self, prefix, suffix, directory="/__os_temp__"):
        path = tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=directory)
        return path

    def delete(self, path, recursive=False, skip_trash=True):
        """
        Dosya siler.
        :return:
        """
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)

    def delete_on_exit(self, path):
        raise Exception("not implemented!")

    def exists(self, path):
        """
        dosya/dizin in var olup olmadığını sorgular
        :param path:
        :return: True yada False döner
        """
        return os.path.exists(path)

    def get_file_info(self, path):

        result = dict()
        result["is_file"] = os.path.isfile(path)
        result["size"] = os.path.getsize(path)

        a = os.stat(path)
        result["st_mode"] = a.st_mode
        result["st_ino"] = a.st_ino
        result["st_nlink"] = a.st_nlink
        result["st_size"] = a.st_size
        result["st_atime"] = a.st_atime
        result["st_mtime"] = a.st_mtime
        result["st_ctime"] = a.st_ctime
        result["path"] = path

        return result

    def get_hash_code(self, path, type="md5", block_size=65536):
        """
        Dosyaya ait hash kodunu doner
        :type: Hash tipini belirtir, default değeri md5 dir, ayrica sha1 olarak da hesaplanabilir
        :block_size: Dosya okuyacagi chunk buyukluklerini belirtir,
        dosya boyutuna gore guncellenebilir, default is 64KB
        :return: Dosyaya ait hash kodu
        """
        try:
            if type == "md5":
                hasher = hashlib.md5()
            elif type == "sha1":
                hasher = hashlib.sha1()
            else:
                raise ValueError(
                    "unsupported  hash type, use md5 or sha1!"
                )

            with open(path, "rb") as afile:
                buf = afile.read(block_size)

                while len(buf) > 0:
                    hasher.update(buf)
                    buf = afile.read(block_size)

            return hasher.hexdigest()

        except FileNotFoundError:
            raise Exception("Hash bilgisi alinacak dosya bulunamadi !")
        except Exception as e:
            raise Exception(
                "Dosyanin hash kodunu bulurken hata ile karsilasildi!", sys.exc_info()[0]
            )

    def list_file_names(self, pattern):
        glob_ = glob.glob(pattern, recursive=True)
        result = [x.replace("\\", "/") for x in glob_]

        return result

    def list_file_info(self, pattern):
        result = []
        for file in self.list_file_names(pattern):
            result.append(self.get_file_info(file))

        return result

    def rename(self, src_path, dest_path):
        """
        Dosyanın ismini günceller
        :param new_name: yeni dosya ismi
        :return:
        """
        try:
            return os.rename(src_path, dest_path)
        except PermissionError as err:
            print("Dosyanin ismi guncellenirken yetki hatasi ile karsilasildi !", err)
            return None
        except Exception:
            print(
                "Dosyanin ismi guncellenirken hata ile karsilasildi!", sys.exc_info()[0]
            )
            return None

    def write_csv(self, path, dataframe, **kwargs):
        """
        dataframe içeriğini CSV olarak yazar. Pandas/Spark DF nesne tiplerini kendisi kontrol eder
        :param path: local path
        :param dataframe: pandas yada spark df
        :param kwargs: tamamlayıcı bileşenler
        """
        result = None
        self._check_save_mode(dataframe, kwargs)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            result = dataframe.to_csv(path, **kwargs)
        else:
            raise Exception(f"unsupported :{type(dataframe)}")

        return result

    def write_excel(self, path, dataframe, **kwargs):
        """
        dataframe içeriğini Excel olarak yazar. Pandas/Spark DF nesne tiplerini kendisi kontrol eder
        :param path: local path
        :param dataframe: pandas yada spark df
        :param kwargs: tamamlayıcı bileşenler
        """
        result = None
        self._check_save_mode(dataframe, kwargs)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            result = dataframe.to_excel(path, **kwargs)
        else:
            raise Exception(f"unsupported :{type(dataframe)}")

        return result

    def write_json(self, path, dataframe, orient="records", **kwargs):
        """
        dataframe içeriğini Json olarak yazar. Pandas/Spark DF nesne tiplerini kendisi kontrol eder
        :param path: local path
        :param dataframe: pandas yada spark df
        :param kwargs: tamamlayıcı bileşenler
        """
        result = None
        self._check_save_mode(dataframe, kwargs)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            out = dataframe.to_json(orient=orient, **kwargs)
            with open(path, "wb") as file:  # pylint: disable=W1514
                file.write(out)
        else:
            raise Exception(f"unsupported :{type(dataframe)}")

        return result

    def write_parquet(self, path, dataframe, **kwargs):
        """
        dataframe içeriğini Parquet olarak yazar.
        Pandas/Spark DF nesne tiplerini kendisi kontrol eder
        :param path: local path
        :param dataframe: pandas yada spark df
        :param kwargs: tamamlayıcı bileşenler
        """
        result = None
        self._check_save_mode(dataframe, kwargs)

        if isinstance(dataframe, pd.DataFrame):
            if "save_mode" in kwargs:
                raise Exception("save_mode, spark_df için geçerlidir")
            dataframe.to_parquet(path, **kwargs)
        else:
            raise Exception(f"unsupported :{type(dataframe)}")

        return result

    def read_csv(self, path, **kwargs):
        kwargs["header"] = 0 if kwargs["header"] == True else None
        result = pd.read_csv(path, **kwargs)
        return result

    def read_excel(self, path, **kwargs):
        result = pd.read_excel(path, **kwargs)
        if kwargs is not None and "astype" in kwargs:
            result = result.astype(kwargs["astype"])
        return result

    def read_json(self, path, orient="records", **kwargs):
        result = pd.read_json(path, orient, **kwargs)
        return result

    def read_parquet(self, path, **kwargs):
        result = pd.read_parquet(path, **kwargs)
        return result

    def mkdirs(self, path, permission=0o777, exist_ok=False):

        if isinstance(permission, str):
            permission = int(permission, 8)

        return os.makedirs(path, mode=permission, exist_ok=exist_ok)

    def write_txt(self, path, data, mode="wb"):
        with open(path, mode) as txt_file:  # pylint: disable=W1514
            txt_file.write(data)

    def write_bin(self, path, data, mode="wb"):
        with open(path, mode) as bin_file:
            bin_file.write(data)

    def read_xml(self, path, **kwargs):
        result = ""
        tree = ET.parse(path)
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
            with open(path, "wt") as file:  # pylint: disable=W1514
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
        with ZipFile(dest, "w") as zip:
            if os.path.isdir(source):
                for folder, _subfolders, filenames in os.walk(source):
                    for filename in filenames:
                        filepath = os.path.join(folder, filename)
                        relative_path = folder.replace("\\", "/").replace(
                            source.replace("\\", "/"), ""
                        )
                        zip_path = os.path.join(relative_path, filename)
                        zip.write(filepath, zip_path)
            else:
                zip.write(source, os.path.basename(source))

    def unzip(self, source, dest, print_dir=False):
        with ZipFile(source, "r") as zip:
            if print_dir:
                zip.printdir()
            zip.extractall(path=dest)

    def copy(self, source, dest):
        shutil.copy(source, dest)

    def move(self, source, dest):
        shutil.move(source, dest)

    def read_txt(self, path):
        result = ""
        with open(path, "rb") as txt:
            result = txt.read()

        return result

    def read_binary(self, path):
        result = None
        with open(path, "rb") as binary:
            result = binary.read()

        return result
