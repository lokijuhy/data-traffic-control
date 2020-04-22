import datetime
import inspect
import os
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

from datatc.data_interface import DataInterfaceManager, DillDataInterface, TextDataInterface
from datatc import git_utilities

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from datatc.data_directory import DataDirectory, TransformedDataDirectory, DataFile


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
        print(self.code)

    @classmethod
    def save(cls, data: Any, transformer_func: Callable, data_directory: 'DataDirectory', file_name: str,
             enforce_clean_git=True) -> Path:
        """
        Alternative public method for saving a TransformedData.

        Example Usage:
            dm = DataManager('path')
            fe_dir = dm['feature_engineering']

            TransformedData.save(df, transformer, fe_dir, 'v2.csv')
        """
        return TransformedDataInterface.save(data, transformer_func, data_directory, file_name, enforce_clean_git)

    @classmethod
    def load(cls, transformed_data_dir: 'TransformedDataDirectory', data_interface_hint=None) -> 'TransformedData':
        """
        Alternative public method for loading a TransformedData.

        Example Usage:
            dm = DataManager('path')
            fe_dir = dm['feature_engineering'].latest()
            TransformedData.load(fe_dir)
        """
        return TransformedDataInterface.load(transformed_data_dir, data_interface_hint)


class TransformedDataInterface:

    data_file_name = '_data'

    func_file_name = 'func'
    func_interface = DillDataInterface

    code_file_name = 'code'
    code_interface = TextDataInterface

    @classmethod
    def save(cls, data: Any, transformer_func: Callable, data_directory: 'DataDirectory', file_name: str,
             enforce_clean_git=True) -> Path:
        if enforce_clean_git:
            git_utilities.check_for_uncommitted_git_changes()

        tag, data_file_type = os.path.splitext(file_name)
        transform_dir_name = cls._generate_name_for_transform_dir(tag)
        new_transform_dir_path = Path(data_directory.path, transform_dir_name)
        os.makedirs(new_transform_dir_path)

        data_interface = DataInterfaceManager.select(data_file_type)
        data = transformer_func(data)
        data_interface.save(data, 'data', new_transform_dir_path)

        cls.func_interface.save(transformer_func, 'func', new_transform_dir_path)

        transformer_func_code = inspect.getsource(transformer_func)
        cls.code_interface.save(transformer_func_code, 'code', new_transform_dir_path)

        print('created new file {}'.format(new_transform_dir_path))
        return new_transform_dir_path

    @classmethod
    def load(cls, transformed_data_dir: 'TransformedDataDirectory', data_interface_hint=None) -> 'TransformedData':
        """
        Load a saved data transformer- the data and the function that generated it.

        Args:
            transformed_data_dir: The TransformedDataDirectory that contains the file contents of the TransformedData
            data_interface_hint: Optional, what data interface to use to read the data file.

        Returns: Tuple(data, transformer_func)

        """
        data_file = cls._identify_file_from_contents(transformed_data_dir.contents, 'data', transformed_data_dir.path)
        func_file = cls._identify_file_from_contents(transformed_data_dir.contents, 'func', transformed_data_dir.path)
        code_file = cls._identify_file_from_contents(transformed_data_dir.contents, 'code', transformed_data_dir.path)

        data = data_file.load(data_interface_hint=data_interface_hint)
        transformer_func = func_file.load()
        transformer_code = code_file.load()
        return TransformedData(data, transformer_func, transformer_code)

    @classmethod
    def _generate_name_for_transform_dir(cls, tag: str = None) -> str:
        repo_path = git_utilities.get_repo_path()
        git_hash = git_utilities.get_git_hash(repo_path)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        delimiter_char = '__'
        file_name_components = ['transformed_data_dir', timestamp, git_hash]
        if tag is not None:
            file_name_components.append(tag)
        return delimiter_char.join(file_name_components)

    @classmethod
    def _parse_transform_dir_name(cls, name) -> Tuple[str, str, str]:
        delimiter_char = '__'
        file_name_components = name.split(delimiter_char)
        if len(file_name_components) == 3:
            denoter, timestamp, git_hash = file_name_components
            tag = ''
        elif len(file_name_components) == 4:
            denoter, timestamp, git_hash, tag = file_name_components
        else:
            raise ValueError('TransformedDataDirectory name could not be parsed: {}'.format(name))
        return timestamp, git_hash, tag

    @staticmethod
    def _identify_file_from_contents(contents: Dict[str, 'DataFile'], file_name: str, parent_dir: str) -> 'DataFile':
        options = [contents[file] for file in contents if file_name in file]
        if len(options) == 0:
            raise ValueError('No {} file found for TransformedDataDirectory {}'.format(file_name, parent_dir))
        elif len(options) > 1:
            raise ValueError('More than one {} file found for TransformedDataDirectory {}'.format(file_name, parent_dir)
                             )
        else:
            return options[0]
