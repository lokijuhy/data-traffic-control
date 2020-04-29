import datetime
import glob
import inspect
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from datatc.data_interface import DataInterfaceManager, DillDataInterface, TextDataInterface
from datatc import git_utilities

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from datatc.data_directory import DataDirectory, TransformedDataDirectory


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
        if self.transformer_func is not None:
            return self.transformer_func(*args, **kwargs)
        else:
            raise ValueError('TransformedData Function was not loaded')

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
        # TODO: convert DataDirectory to path/str
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

    file_component_interfaces = {
        'data': None,
        'func': DillDataInterface,
        'code': TextDataInterface,
    }

    @classmethod
    def save(cls, data: Any, transformer_func: Callable, parent_path: str, file_name: str, enforce_clean_git=True)\
            -> Path:
        """Save a transformed dataset.

        Args:
            data: Input data to transform.
            transformer_func: Transform function to apply to data.
            parent_path: The parent path at which the new TransformedDataDirectory will be created.
            file_name: The name will be converted into the tag, and the extension used to determine the type to save the
             data as.
            enforce_clean_git: Whether to only allow the save to proceed if the working state of the git directory is
                clean.

        Returns: Tuple[new transform directory name, TransformedDataDirectory object], for adding to contents dict."""
        if enforce_clean_git:
            git_utilities.check_for_uncommitted_git_changes_at_path(git_utilities.get_repo_path())

        tag, data_file_type = os.path.splitext(file_name)
        transform_dir_name = cls._generate_name_for_transform_dir(tag)
        new_transform_dir_path = Path(parent_path, transform_dir_name)
        os.makedirs(new_transform_dir_path)

        data_interface = DataInterfaceManager.select(data_file_type)
        data = transformer_func(data)
        data_interface.save(data, 'data', new_transform_dir_path)

        cls.file_component_interfaces['func'].save(transformer_func, 'func', new_transform_dir_path)

        transformer_func_code = inspect.getsource(transformer_func)
        cls.file_component_interfaces['code'].save(transformer_func_code, 'code', new_transform_dir_path)

        print('created new file {}'.format(new_transform_dir_path))
        return new_transform_dir_path

    @classmethod
    def load(cls, path: str, data_interface_hint=None, load_function: bool = True) -> 'TransformedData':
        """
        Load a saved data transformer- the data and the function that generated it.

        Args:
            path: The path to the directory that contains the file contents of the TransformedData
            data_interface_hint: Optional, what data interface to use to read the data file.
            load_function: Whether to load the dill'ed function. May want to not load if dependencies are not present in
             current environment, which would cause a ModuleNotFoundError.

        Returns: Tuple(data, transformer_func)

        """
        file_map = cls._identify_transform_sub_files(path)
        data_file = file_map['data']
        func_file = file_map['func']
        code_file = file_map['code']

        data_interface = DataInterfaceManager.select(data_file, default_file_type=data_interface_hint)
        data = data_interface.load(data_file)
        if load_function:
            transformer_func = cls.file_component_interfaces['func'].load(func_file)
        else:
            transformer_func = None
        transformer_code = cls.file_component_interfaces['code'].load(code_file)
        return TransformedData(data, transformer_func, transformer_code)

    @classmethod
    def get_info(cls, path: str) -> Dict[str, str]:
        timestamp, git_hash, tag = cls._parse_transform_dir_name(path)
        file_map = cls._identify_transform_sub_files(path)
        data_file_root, data_file_type = os.path.splitext(file_map['data'])
        return {
            'timestamp': timestamp,
            'git_hash': git_hash,
            'tag': tag,
            'data_type': data_file_type
        }

    @classmethod
    def _identify_transform_sub_files(cls, path: str) -> Dict[str, Path]:
        glob_path = Path(path, '*')
        subpaths = glob.glob(glob_path.__str__())
        file_map = {}
        for file_component in cls.file_component_interfaces:
            file_map[file_component] = cls._identify_sub_file(subpaths, file_component)
        return file_map

    @classmethod
    def _identify_sub_file(cls, file_contents: List[Path], key: str) -> Path:
        options = [file_path for file_path in file_contents if key in os.path.basename(file_path)]
        if len(options) == 0:
            raise ValueError('No {} file found for TransformedData'.format(key))
        elif len(options) > 1:
            raise ValueError('More than one {} file found for TransformedData: {}'.format(key, ', '.join(options)))
        else:
            return options[0]

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
    def _parse_transform_dir_name(cls, path) -> Tuple[str, str, str]:
        delimiter_char = '__'
        dir_name = os.path.basename(path)
        dir_name_components = dir_name.split(delimiter_char)
        if len(dir_name_components) == 3:
            denoter, timestamp, git_hash = dir_name_components
            tag = ''
        elif len(dir_name_components) == 4:
            denoter, timestamp, git_hash, tag = dir_name_components
        else:
            raise ValueError('TransformedDataDirectory name could not be parsed: {}'.format(dir_name))
        return timestamp, git_hash, tag
