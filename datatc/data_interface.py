import pandas as pd
from pathlib import Path
import pickle
import dill
from typing import Any
import yaml


class DataInterfaceBase:
    """
    Govern how a data type is saved and loaded. This class is a base class for all DataInterfaces.
    """

    file_extension = None

    @classmethod
    def save(cls, data: Any, file_name: str, file_dir_path: str, mode: str = None) -> None:
        file_path = cls.construct_file_path(file_name, file_dir_path)
        if mode is None:
            return cls._interface_specific_save(data, file_path)
        else:
            return cls._interface_specific_save(data, file_path, mode)

    @classmethod
    def construct_file_path(cls, file_name: str, file_dir_path: str) -> str:
        return str(Path(file_dir_path, "{}.{}".format(file_name, cls.file_extension)))

    @classmethod
    def _interface_specific_save(cls, data: Any, file_path, mode: str = None) -> None:
        raise NotImplementedError

    @classmethod
    def load(cls, file_path: str) -> Any:
        # file_path = cls.construct_file_path(file_name, file_dir_path)
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(file_path)
        return cls._interface_specific_load(file_path)

    @classmethod
    def _interface_specific_load(cls, file_path) -> Any:
        raise NotImplementedError


class TextDataInterface(DataInterfaceBase):

    file_extension = 'txt'

    @classmethod
    def _interface_specific_save(cls, data, file_path, mode='w'):
        with open(file_path, mode) as f:
            f.write(data)

    @classmethod
    def _interface_specific_load(cls, file_path):
        with open(file_path, 'r') as f:
            file = f.read()
        return file


class PickleDataInterface(DataInterfaceBase):

    file_extension = 'pkl'

    @classmethod
    def _interface_specific_save(cls, data: Any, file_path, mode='wb+') -> None:
        with open(file_path, mode) as f:
            pickle.dump(data, f)

    @classmethod
    def _interface_specific_load(cls, file_path) -> Any:
        with open(file_path, "rb+") as f:
            return pickle.load(f)


class DillDataInterface(DataInterfaceBase):

    file_extension = 'dill'

    @classmethod
    def _interface_specific_save(cls, data: Any, file_path, mode='wb+') -> None:
        with open(file_path, mode) as f:
            dill.dump(data, f)

    @classmethod
    def _interface_specific_load(cls, file_path) -> Any:
        with open(file_path, "rb+") as f:
            return dill.load(f)


class CSVDataInterface(DataInterfaceBase):

    file_extension = 'csv'

    @classmethod
    def _interface_specific_save(cls, data, file_path, mode=None):
        data.to_csv(file_path)

    @classmethod
    def _interface_specific_load(cls, file_path):
        return pd.read_csv(file_path)


class ExcelDataInterface(DataInterfaceBase):

    file_extension = 'xlsx'

    @classmethod
    def _interface_specific_save(cls, data, file_path, mode=None):
        data.to_excel(file_path)

    @classmethod
    def _interface_specific_load(cls, file_path):
        return pd.read_excel(file_path)


class PDFDataInterface(DataInterfaceBase):

    file_extension = 'pdf'

    @classmethod
    def _interface_specific_save(cls, doc, file_path, mode=None):
        doc.save(file_path, garbage=4, deflate=True, clean=True)

    @classmethod
    def _interface_specific_load(cls, file_path):
        import fitz
        return fitz.open(file_path)


class YAMLDataInterface(DataInterfaceBase):

    file_extension = 'yaml'

    @classmethod
    def _interface_specific_save(cls, data, file_path, mode='w'):
        # TODO: make mode an arg in all saves
        with open(file_path, mode) as f:
            yaml.dump(data, f, default_flow_style=False)

    @classmethod
    def _interface_specific_load(cls, file_path):
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
        return data


class DataInterfaceManager:

    file_extension = None
    registered_interfaces = {
        'pkl': PickleDataInterface,
        'dill': DillDataInterface,
        'csv': CSVDataInterface,
        'xlsx': ExcelDataInterface,
        'txt': TextDataInterface,
        'sql': TextDataInterface,
        'pdf': PDFDataInterface,
        'yaml': YAMLDataInterface,
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
