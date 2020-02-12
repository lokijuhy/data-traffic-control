import glob
import inspect
import os
from pathlib import Path
from typing import Any, Callable

import datatc.data_interface as di


class DataProcessor:

    def __init__(self, data, processor_func, code):
        self.data_set = data
        self.processor_func = processor_func
        self.code = code

    @property
    def data(self):
        return self.data_set

    @property
    def func(self):
        return self.processor_func

    def rerun(self, *args, **kwargs):
        return self.processor_func(*args, **kwargs)

    def view_code(self):
        return self.code


class DataProcessorCacheManager:

    def __init__(self):
        self.processor_designation = '_processor'
        self.processor_data_interface = di.DillDataInterface
        self.code_designation = '_code'
        self.code_data_interface = di.TextDataInterface

    def save(self, data: Any, processing_func: Callable, file_name: str, data_file_type: str, file_dir_path: str):
        if self.check_name_already_exists(file_name, file_dir_path):
            raise ValueError("That data processor name is already in use")

        data_interface = di.DataInterfaceManager.select(data_file_type)
        data = processing_func(data)
        data_interface.save(data, file_name, file_dir_path)
        self.processor_data_interface.save(processing_func, file_name + self.processor_designation, file_dir_path)
        processing_func_code = inspect.getsource(processing_func)
        self.code_data_interface.save(processing_func_code, file_name + self.code_designation, file_dir_path)

    def load(self, file_name: str, file_dir_path: str) -> DataProcessor:
        """
        Load a cached data processor- the data and the function that generated it.
        Accepts a file name with or without a file extension.
        Args:
            file_name: The base name of the data file. May include the file extension, otherwise the file extension
                will be deduced.
            file_dir_path: the path to the directory where cached data processors are stored.
        Returns: Tuple(data, processing_func)
        """
        data_file_extension = None
        if '.' in file_name:
            file_name, data_file_extension = file_name.split('.')

        # load the processor
        processing_func = self.processor_data_interface.load(file_name + self.processor_designation, file_dir_path)

        # load the data
        code = self.code_data_interface.load(file_name + self.code_designation, file_dir_path)

        # find and load the data
        if data_file_extension is None:
            data_file_extension = self.get_data_processor_data_type(file_name, file_dir_path)
        data_interface = di.DataInterfaceManager.select(data_file_extension)
        data = data_interface.load(file_name, file_dir_path)
        return DataProcessor(data, processing_func, code)

    def check_name_already_exists(self, file_name, file_dir_path):
        existing_data_processors = self.list_cached_data_processors(file_dir_path)
        if file_name in existing_data_processors:
            return True
        else:
            return False

    @staticmethod
    def get_data_processor_data_type(file_name, file_dir_path):
        data_path = Path("{}/{}.*".format(file_dir_path, file_name))
        processor_files = glob.glob(data_path.__str__())

        if len(processor_files) == 0:
            raise ValueError("No data file found for processor {}".format(file_name))
        elif len(processor_files) > 1:
            raise ValueError("Something went wrong- there's more than one file that matches this processor name: "
                             "{}".format("\n - ".join(processor_files)))

        data_file = processor_files[0]
        data_file_extension = os.path.basename(data_file).split('.')[1]
        return data_file_extension

    def list_cached_data_processors(self, file_dir_path: str):
        # glob for all processor files
        processors_path = Path("{}/*{}.{}".format(file_dir_path, self.processor_designation,
                                                  self.processor_data_interface.file_extension))
        processor_files = glob.glob(processors_path.__str__())
        processor_names = [os.path.basename(file).split('.')[0] for file in processor_files]
        return processor_names
