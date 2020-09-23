import pandas as pd
from pathlib import Path
import pickle
import dill
import os
from typing import Any
import yaml


class DataInterfaceBase:
    """
    Govern how a data type is saved and loaded. This class is a base class for all DataInterfaces.
    """

    file_extension = None

    @classmethod
    def save(cls, data: Any, file_name: str, file_dir_path: str, mode: str = None, **kwargs) -> str:
        file_path = cls.construct_file_path(file_name, file_dir_path)
        if mode is None:
            cls._interface_specific_save(data, file_path, **kwargs)
        else:
            cls._interface_specific_save(data, file_path, mode, **kwargs)
        return file_path

    @classmethod
    def construct_file_path(cls, file_name: str, file_dir_path: str) -> str:
        root, ext = os.path.splitext(file_name)
        if ext == '':
            return str(Path(file_dir_path, "{}.{}".format(file_name, cls.file_extension)))
        else:
            return str(Path(file_dir_path, file_name))

    @classmethod
    def _interface_specific_save(cls, data: Any, file_path, mode: str = None, **kwargs) -> None:
        raise NotImplementedError

    @classmethod
    def load(cls, file_path: str, **kwargs) -> Any:
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(file_path)
        return cls._interface_specific_load(str(file_path), **kwargs)

    @classmethod
    def _interface_specific_load(cls, file_path, **kwargs) -> Any:
        raise NotImplementedError


class TextDataInterface(DataInterfaceBase):

    file_extension = 'txt'

    @classmethod
    def _interface_specific_save(cls, data, file_path, mode='w', **kwargs):
        with open(file_path, mode, **kwargs) as f:
            f.write(data)

    @classmethod
    def _interface_specific_load(cls, file_path, **kwargs):
        with open(file_path, 'r', **kwargs) as f:
            file = f.read()
        return file


class PickleDataInterface(DataInterfaceBase):

    file_extension = 'pkl'

    @classmethod
    def _interface_specific_save(cls, data: Any, file_path, mode='wb+', **kwargs) -> None:
        with open(file_path, mode, **kwargs) as f:
            pickle.dump(data, f)

    @classmethod
    def _interface_specific_load(cls, file_path, **kwargs) -> Any:
        with open(file_path, "rb+", **kwargs) as f:
            return pickle.load(f)


class DillDataInterface(DataInterfaceBase):

    file_extension = 'dill'

    @classmethod
    def _interface_specific_save(cls, data: Any, file_path, mode='wb+', **kwargs) -> None:
        with open(file_path, mode, **kwargs) as f:
            dill.dump(data, f)

    @classmethod
    def _interface_specific_load(cls, file_path, **kwargs) -> Any:
        with open(file_path, "rb+", **kwargs) as f:
            return dill.load(f)


class CSVDataInterface(DataInterfaceBase):

    file_extension = 'csv'

    @classmethod
    def _interface_specific_save(cls, data, file_path, mode=None, **kwargs):
        data.to_csv(file_path, **kwargs)

    @classmethod
    def _interface_specific_load(cls, file_path, **kwargs):
        return pd.read_csv(file_path, **kwargs)


class ExcelDataInterface(DataInterfaceBase):

    file_extension = 'xlsx'

    @classmethod
    def _interface_specific_save(cls, data, file_path, mode=None, **kwargs):
        data.to_excel(file_path, **kwargs)

    @classmethod
    def _interface_specific_load(cls, file_path, **kwargs):
        return pd.read_excel(file_path, **kwargs)


class ParquetDataInterface(DataInterfaceBase):

    file_extension = 'parquet'

    @classmethod
    def _interface_specific_save(cls, data, file_path, mode=None, **kwargs):
        data.to_parquet(file_path, **kwargs)

    @classmethod
    def _interface_specific_load(cls, file_path, **kwargs):
        return pd.read_parquet(file_path, **kwargs)


class PDFDataInterface(DataInterfaceBase):

    file_extension = 'pdf'

    @classmethod
    def _interface_specific_save(cls, doc, file_path, mode=None, **kwargs):
        doc.save(file_path, garbage=4, deflate=True, clean=True, **kwargs)

    @classmethod
    def _interface_specific_load(cls, file_path, **kwargs):
        import fitz
        return fitz.open(file_path, **kwargs)


class YAMLDataInterface(DataInterfaceBase):

    file_extension = 'yaml'

    @classmethod
    def _interface_specific_save(cls, data, file_path, mode='w', **kwargs):
        with open(file_path, mode, **kwargs) as f:
            yaml.dump(data, f, default_flow_style=False)

    @classmethod
    def _interface_specific_load(cls, file_path, **kwargs):
        with open(file_path, 'r', **kwargs) as f:
            data = yaml.safe_load(f)
        return data


class TestingDataInterface(DataInterfaceBase):
    """Test class that doesn't make interactions with the file system, for use in unit tests"""

    file_extension = 'test'

    @classmethod
    def _interface_specific_save(cls, data, file_path, mode='wb+', **kwargs) -> None:
        return

    @classmethod
    def _interface_specific_load(cls, file_path, **kwargs):
        return {'data': 42}


class DataInterfaceManager:

    registered_interfaces = {
        'pkl': PickleDataInterface,
        'dill': DillDataInterface,
        'csv': CSVDataInterface,
        'parquet': ParquetDataInterface,
        'xlsx': ExcelDataInterface,
        'txt': TextDataInterface,
        'sql': TextDataInterface,
        'pdf': PDFDataInterface,
        'yaml': YAMLDataInterface,

        # for unit testing purposes only
        'test': TestingDataInterface,
    }

    @classmethod
    def instantiate_data_interface(cls, file_type: str) -> DataInterfaceBase:
        if file_type in cls.registered_interfaces:
            return cls.registered_interfaces[file_type]()
        else:
            raise ValueError("File type {} not recognized. Supported file types include {}".format(
                file_type, list(cls.registered_interfaces.keys())))

    @classmethod
    def parse_file_hint(cls, file_hint: str) -> str:
        if '.' in file_hint:
            file_name, file_extension = file_hint.split('.')
            return file_extension
        else:
            return file_hint

    @classmethod
    def select(cls, file_hint: str, default_file_type=None) -> DataInterfaceBase:
        """
        Select the appropriate data interface based on the file_hint.

        Args:
            file_hint: May be a file name with an extension, or just a file extension.
            default_file_type: default file type to use, if the file_hint doesn't specify.
        Returns: A DataInterface.
        """
        file_hint = cls.parse_file_hint(file_hint)
        if file_hint in cls.registered_interfaces:
            return cls.instantiate_data_interface(file_hint)
        elif default_file_type is not None:
            return cls.instantiate_data_interface(default_file_type)
        else:
            raise ValueError("File hint {} not recognized. Supported file types include {}".format(
                file_hint, list(cls.registered_interfaces.keys())))
