from copy import deepcopy
import datetime
import glob
import inspect
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple, Union

from datatc.data_interface import DataInterfaceManager, DillDataInterface, TextDataInterface
from datatc.git_utilities import get_git_repo_of_func, check_for_uncommitted_git_changes_at_path, get_git_hash_from_path


class TransformStep:

    def __init__(self, data: Any, transformer_func: Callable, tag: str = '', enforce_clean_git: bool = True,
                 get_git_hash_from: Any = None, **kwargs):

        self.tag = tag
        self.transformer_func = transformer_func
        self.kwargs = kwargs
        self.code = inspect.getsource(transformer_func)
        self.git_hash = self.get_git_hash(transformer_func, get_git_hash_from, enforce_clean_git)
        self.data = transformer_func(data, **kwargs)

    def rerun(self, data: Any) -> Any:
        return self.transformer_func(data, **self.kwargs)

    @staticmethod
    def get_git_hash(transformer_func, get_git_hash_from, enforce_clean_git: bool = True) -> Union[str, None]:
        """

        Args:
            transformer_func:
            enforce_clean_git: Whether to only allow the save to proceed if the working state of the git directory is
                clean.
            get_git_hash_from: Locally installed module from which to get git information. Use this arg if
                transform_func is defined outside of a module tracked by git.

        Returns: string of the git hash if the transformer_func is in a repo, otherwise None.

        Raises: RuntimeError if there are uncommitted changes in the transformer_func's repo. Can be overrridden by
            `enforce_clean_git = False`.
        """
        if get_git_hash_from:
            transformer_func_file_repo_path = get_git_repo_of_func(get_git_hash_from)
        else:
            transformer_func_file_repo_path = get_git_repo_of_func(transformer_func)
        transformer_func_in_repo = True if transformer_func_file_repo_path else False

        if enforce_clean_git:
            if transformer_func_in_repo:
                check_for_uncommitted_git_changes_at_path(transformer_func_file_repo_path)
            else:
                raise RuntimeError('`transformer_func` is not tracked in a git repo.'
                                   'Use `enforce_clean_git=False` to override this restriction.')

        git_hash = get_git_hash_from_path(transformer_func_file_repo_path) if transformer_func_in_repo else None
        return git_hash

    def get_info(self):
        return {
            'tag': self.tag,
            'code': self.code,
            'kwargs': self.kwargs,
            'git_hash': self.git_hash,
        }


class TransformSequence:

    def __init__(self):
        self.sequence = []

    def append(self, transform_step: TransformStep):
        self.sequence.append(transform_step)

    def rerun(self, data: Any) -> Any:
        transformed_data = deepcopy(data)
        for step in self.sequence:
            transformed_data = step.rerun(transformed_data)
        return transformed_data

    def is_not_empty(self):
        return len(self.sequence) > 0

    def get_last_data(self):
        return self.sequence[-1].data

    def get_info(self):
        info_list = []
        for step in self.sequence:
            info_list.append(step.get_info())
        return info_list

    def view_code(self):
        for i, step in enumerate(self.sequence):
            print("Step {}".format(i))
            print("-"*80)
            print(step.code)
            print()


class SelfAwareData:
    """A wrapper around a dataset that also contains the code that generated the data.
     `SelfAwareData` can re-run it's transformer functions on a new dataset."""

    def __init__(self, data: Any):
        self.start_data = data
        self.transform_sequence = TransformSequence()

    @property
    def data(self) -> Any:
        if self.transform_sequence.is_not_empty():
            return self.transform_sequence.get_last_data()
        else:
            return self.start_data

    def get_info(self):
        return self.transform_sequence.get_info()

    def transform(self, transformer_func: Callable, tag: str = '', enforce_clean_git=True,
                  get_git_hash_from: Any = None, **kwargs):
        """
        Transform a SelfAwareData, generating a new SelfAwareData object.

        Args:
            transformer_func: Transform function to apply to data.
            tag: (optional) short description of the transform for reference
            enforce_clean_git: Whether to only allow the save to proceed if the working state of the git directory is
                clean.
            get_git_hash_from: Locally installed module from which to get git information. Use this arg if
                transform_func is defined outside of a module tracked by git.

        Returns: new transform directory name, for adding to contents dict.
        """
        transform_step = TransformStep(self.data, transformer_func, tag, enforce_clean_git=enforce_clean_git,
                                       get_git_hash_from=get_git_hash_from, **kwargs)
        self.transform_sequence.append(transform_step)

    def rerun(self, data) -> Any:
        """
        Rerun the same transformation function that generated this `SelfAwareData` on a new data object.
        Args:
            data:

        Returns:

        """
        return self.transform_sequence.rerun(data)

    def view_code(self):
        """Print the code of the transformation steps that generated the data."""
        self.transform_sequence.view_code()

    def save(self, file_path: str,  **kwargs) -> Path:
        """
        Save a SelfAwareData object.

        Args:
            file_path: Path for where to save the SAD, including file extension.

        Returns: Path to the saved SAD

        Example Usage:
            >>> processed_sad = raw_sad.transform(build_features, 'standard_features')
            >>> processed_sad.save('~/project/data/standard_features.csv')
        """
        dir_path = os.path.dirname(file_path)
        file_name = os.path.basename(file_path)
        return SelfAwareDataInterface.save(self, dir_path, file_name, **kwargs)

    @classmethod
    def load(cls, file_path: str, data_interface_hint=None, **kwargs) -> 'SelfAwareData':
        """
        Load a SelfAwareData object.

        Args:
            file_path: Path to the SAD to load.

        Example Usage:
            >>> sad = SelfAwareData.load('~/project/data/sad_dir__2021-01-01_12-00__standard_features.csv')
        """
        return SelfAwareDataInterface.load(file_path, data_interface_hint, **kwargs)


class SelfAwareDataInterface:
    """DataInterface for saving and loading `SelfAwareData` objects."""

    file_component_interfaces = {
        'data': None,
        'func': DillDataInterface,
        'code': TextDataInterface,
    }

    @classmethod
    def save(cls, sad: SelfAwareData, parent_path: str, file_name: str,  **kwargs) -> Path:
        """
        Save a SelfAwareData object.

        Args:
            sad: a SelfAwareData object
            parent_path: The parent path at which the new SelfAwareDataDirectory will be created.
            file_name: The name will be converted into the tag, and the extension used to determine the type to save the
             data as.

        Returns: new transform directory name, for adding to contents dict.
        """

        tag, data_file_type = os.path.splitext(file_name)
        transform_dir_name = cls._generate_name_for_transform_dir(tag, sad.git_hash)
        new_transform_dir_path = Path(parent_path, transform_dir_name)
        os.makedirs(new_transform_dir_path)

        data_interface = DataInterfaceManager.select(data_file_type)
        data_interface.save(sad.data, 'data', new_transform_dir_path, **kwargs)

        cls.file_component_interfaces['func'].save(sad.transformer_func, 'func', new_transform_dir_path)

        cls.file_component_interfaces['code'].save(sad.code, 'code', new_transform_dir_path)

        print('created new file {}'.format(new_transform_dir_path))
        return new_transform_dir_path

    @classmethod
    def load(cls, path: str, data_interface_hint=None, load_function: bool = True, **kwargs) -> 'SelfAwareData':
        """
        Load a saved data transformer- the data and the function that generated it.

        Args:
            path: The path to the directory that contains the file contents of the SelfAwareData
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
        data = data_interface.load(data_file, **kwargs)
        if load_function:
            transformer_func = cls.file_component_interfaces['func'].load(func_file)
        else:
            transformer_func = None
        transformer_code = cls.file_component_interfaces['code'].load(code_file)
        info = cls.get_info(path)
        return SelfAwareData(data, transformer_func, transformer_code, info)

    @classmethod
    def get_info(cls, path: str) -> Dict[str, str]:
        timestamp, git_hash, tag = cls._parse_transform_dir_name(path)
        file_map = cls._identify_transform_sub_files(path)
        data_file_root, data_file_type = os.path.splitext(file_map['data'])
        data_file_type = data_file_type.replace('.', '')
        return {
            'timestamp': cls._parse_timestamp_for_printing(timestamp),
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
            raise ValueError('No {} file found for SelfAwareData'.format(key))
        elif len(options) > 1:
            raise ValueError('More than one {} file found for SelfAwareData: {}'.format(key, ', '.join(options)))
        else:
            return options[0]

    @classmethod
    def _generate_name_for_transform_dir(cls, tag: str = None, git_hash: str = None) -> str:
        timestamp = cls.generate_timestamp()
        delimiter_char = '__'
        file_name_components = ['sad_dir', timestamp, git_hash]
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
            raise ValueError('SelfAwareDataDirectory name could not be parsed: {}'.format(dir_name))
        return timestamp, git_hash, tag

    @classmethod
    def generate_timestamp(cls):
        return datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    @classmethod
    def _parse_timestamp_for_printing(cls, timestamp):
        date, time = timestamp.split('_')
        hours, minutes, seconds = time.split('-')
        return '{} {}:{}'.format(date, hours, minutes)
