import inspect
import glob
import os
from pathlib import Path
from typing import Any, Callable

from datatc.data_interface import DataInterfaceManager, DillDataInterface, TextDataInterface


class TransformedData:
    """A wrapper around a dataset that also contains the code that generated the data.
     TransformedData can re-run it's transformer function on a new dataset."""

    def __init__(self, data, transformer_func, code):
        self.data_set = data
        self.transformer_func = transformer_func
        self.code = code

    @property
    def data(self):
        return self.data_set

    @property
    def func(self):
        return self.transformer_func

    def rerun(self, *args, **kwargs):
        return self.transformer_func(*args, **kwargs)

    def view_code(self):
        return self.code


class TransformedDataInterface:

    def __init__(self):
        self.data_designation = '_data'
        self.transformer_data_interface = DillDataInterface
        self.func_designation = '_func'
        self.transformer_func_interface = TextDataInterface

    def save(self, data: Any, transformer_func: Callable, file_name: str, data_file_type: str, file_dir_path: str):
        """

        Args:
            data:
            transformer_func:
            file_name:
            data_file_type:
            file_dir_path:

        Returns:

        """
        if self.check_name_already_exists(file_name, file_dir_path):
            raise ValueError("That data transformer name is already in use")

        data_interface = DataInterfaceManager.select(data_file_type)
        data = transformer_func(data)
        data_interface.save(data, file_name, file_dir_path)
        self.transformer_data_interface.save(transformer_func, file_name+self.data_designation, file_dir_path)
        transformer_func_code = inspect.getsource(transformer_func)
        self.transformer_func_interface.save(transformer_func_code, file_name+self.func_designation, file_dir_path)

    def load(self, file_name: str, file_dir_path: str) -> TransformedData:
        """
        Load a cached data transformer- the data and the function that generated it.
        Accepts a file name with or without a file extension.

        Args:
            file_name: The base name of the data file. May include the file extension, otherwise the file extension
                will be deduced.
            file_dir_path: the path to the directory where cached data transformers are stored.

        Returns: Tuple(data, transformer_func)

        """
        data_file_extension = None
        if '.' in file_name:
            file_name, data_file_extension = file_name.split('.')

        # load the transformer
        transformer_func = self.transformer_data_interface.load(file_name+self.data_designation, file_dir_path)

        # load the data
        code = self.transformer_func_interface.load(file_name+self.func_designation, file_dir_path)

        # find and load the data
        if data_file_extension is None:
            data_file_extension = self.get_data_transformer_data_type(file_name, file_dir_path)
        data_interface = DataInterfaceManager.select(data_file_extension)
        data = data_interface.load(file_name, file_dir_path)
        return TransformedData(data, transformer_func, code)

    def check_name_already_exists(self, file_name, file_dir_path):
        existing_data_transformers = self.list_saved_data_transformers(file_dir_path)
        if file_name in existing_data_transformers:
            return True
        else:
            return False

    @staticmethod
    def get_data_transformer_data_type(file_name, file_dir_path):
        data_path = Path("{}/{}.*".format(file_dir_path, file_name))
        transformer_files = glob.glob(data_path.__str__())

        if len(transformer_files) == 0:
            raise ValueError("No data file found for transformer {}".format(file_name))
        elif len(transformer_files) > 1:
            raise ValueError("Something went wrong- there's more than one file that matches this transformer name: "
                             "{}".format("\n - ".join(transformer_files)))

        data_file = transformer_files[0]
        data_file_extension = os.path.basename(data_file).split('.')[1]
        return data_file_extension

    def list_saved_data_transformers(self, file_dir_path: str):
        # glob for all transformer files
        transformers_path = Path("{}/*{}.{}".format(file_dir_path, self.data_designation,
                                                    self.transformer_data_interface.file_extension))
        transformer_files = glob.glob(transformers_path.__str__())
        transformer_names = [os.path.basename(file).split('.')[0] for file in transformer_files]
        return transformer_names
