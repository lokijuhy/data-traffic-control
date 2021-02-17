from copy import deepcopy
import datetime
import glob
import inspect
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from datatc.data_interface import DataInterfaceManager, DillDataInterface, TextDataInterface, YAMLDataInterface
from datatc.git_utilities import get_git_repo_of_func, check_for_uncommitted_git_changes_at_path, get_git_hash_from_path


class SADTimestamp:

    @classmethod
    def now(cls):
        return datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    @classmethod
    def format(cls, timestamp):
        date, time = timestamp.split('_')
        hours, minutes, seconds = time.split('-')
        return '{} {}:{}'.format(date, hours, minutes)


class TransformStepBase:

    def __init__(self, **kwargs):
        self.metadata = {}

    def get_info(self) -> Dict:
        raise NotImplementedError

    def print_step(self, step_no) -> None:
        raise NotImplementedError

    def rerun(self, data: Any):
        raise NotImplementedError


class LiveTransformStep(TransformStepBase):

    def __init__(self, metadata: Dict, transformer_func: Callable):
        """
        Track a data transformation step.

        Args:
            metadata:
            transformer_func:
        """
        if type(metadata) is not dict:
            raise ValueError('metadata must be type dict, received {}'.format(type(metadata)))
        if not callable(transformer_func):
            raise ValueError('transformer_func must be callable, received type{}'.format(type(transformer_func)))

        super().__init__()
        self.metadata = metadata
        self.transformer_func = transformer_func

    @property
    def timestamp(self):
        return self.metadata.get('timestamp', None)

    @property
    def tag(self):
        return self.metadata.get('tag', None)

    @property
    def git_hash(self):
        return self.metadata.get('git_hash', None)

    @property
    def code(self):
        return self.metadata.get('code', None)

    @property
    def kwargs(self):
        return self.metadata.get('kwargs', {})

    def get_info(self):
        return {
            'timestamp': self.timestamp,
            'tag': self.tag,
            'code': self.code,
            'kwargs': self.kwargs,
            'git_hash': self.git_hash,
        }

    def print_step(self, step_no) -> None:
        print("-" * 80)
        print("Step {:>2} {:>30} {:>20}".format(step_no, self.timestamp, self.git_hash))
        print("-" * 80)
        print(self.code)
        if self.kwargs is not None and len(self.kwargs) > 0:
            print()
            for kw in self.kwargs:
                print(' - {}: {}'.format(kw, self.kwargs[kw]))
        print()

    def rerun(self, data: Any) -> Any:
        return self.transformer_func(data, **self.kwargs)


class StaticTransformStep(TransformStepBase):

    def __init__(self, metadata: Dict):
        """
        Track a data transformation step.

        Args:
            metadata:
        """
        if type(metadata) is not dict:
            raise ValueError('metadata must be type dict, received {}'.format(type(metadata)))

        super().__init__()
        self.metadata = metadata

    @property
    def timestamp(self):
        return self.metadata.get('timestamp', None)

    @property
    def tag(self):
        return self.metadata.get('tag', None)

    @property
    def git_hash(self):
        return self.metadata.get('git_hash', None)

    @property
    def code(self):
        return self.metadata.get('code', None)

    @property
    def kwargs(self):
        return self.metadata.get('kwargs', {})

    def get_info(self):
        return {
            'timestamp': self.timestamp,
            'tag': self.tag,
            'code': self.code,
            'kwargs': self.kwargs,
            'git_hash': self.git_hash,
        }

    def print_step(self, step_no) -> None:
        print("-" * 80)
        print("Step {:>2} {:>30} {:>20}".format(step_no, self.timestamp, self.git_hash))
        print("-" * 80)
        print(self.code)
        if self.kwargs is not None and len(self.kwargs) > 0:
            print()
            for kw in self.kwargs:
                print(' - {}: {}'.format(kw, self.kwargs[kw]))
        print()

    def rerun(self, data: Any):
        raise RuntimeError('rerun is not available for StaticTransformSteps')


class FileSourceTransformStep(TransformStepBase):

    def __init__(self, file_path: str):
        """
        Track a data transformation step.

        Args:
            file_path:
        """

        super().__init__()
        self.file_path = file_path

    def get_info(self):
        return {
            'file_path': str(self.file_path),
        }

    def print_step(self, step_no) -> None:
        print("-" * 80)
        print("Step {:>2}".format(step_no))
        print("-" * 80)
        print('Source file: {}'.format(self.file_path))
        print()

    def rerun(self, data: Any):
        raise RuntimeError('rerun is not available for FileSourceTransformSteps')


class TransformStepInterface:

    @classmethod
    def execute(cls, data: Any, transformer_func: Callable, tag: str = '', enforce_clean_git: bool = True,
                get_git_hash_from: Any = None, **kwargs) -> Union[Any, TransformStepBase]:
        code = inspect.getsource(transformer_func)
        git_hash = cls.get_git_hash(transformer_func, get_git_hash_from, enforce_clean_git)
        metadata = {
            'timestamp': SADTimestamp.format(SADTimestamp.now()),
            'code': code,
            'git_hash': git_hash,
            'kwargs': kwargs,
        }
        if tag is not None:
            metadata['tag'] = tag
        data = transformer_func(data, **kwargs)
        return data, LiveTransformStep(metadata, transformer_func)

    @classmethod
    def load(cls, metadata: Dict = None, transform_func: Callable = None, file_path: str = None
             ) -> TransformStepBase:
        """Factory method for generating different subclasses of TransformSteps"""
        if transform_func is not None:
            return cls.load_live(metadata, transform_func)

        elif file_path is not None:
            return cls.from_file_path(file_path)

        else:
            return cls.load_static(metadata)

    @staticmethod
    def load_live(metadata: Dict, transform_func: Callable) -> LiveTransformStep:
        return LiveTransformStep(metadata, transform_func)

    @staticmethod
    def load_static(metadata: Dict) -> StaticTransformStep:
        return StaticTransformStep(metadata)

    @staticmethod
    def from_file_path(file_path: str) -> FileSourceTransformStep:
        return FileSourceTransformStep(file_path)

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

        Raises: RuntimeError if there are uncommitted changes in the transformer_func's repo. Can be overridden by
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

        if transformer_func_in_repo:
            return get_git_hash_from_path(transformer_func_file_repo_path)
        else:
            return 'no_git_hash'


TransformSequenceConvertible = Union['TransformSequence', List[TransformStepBase], List[Dict]]


class TransformSequence:

    def __init__(self, sequence: TransformSequenceConvertible = None):
        if type(sequence) == TransformSequence:
            self.sequence = sequence.sequence
        elif sequence is None:
            self.sequence = []
        elif type(sequence) is list:
            if len(sequence) == 0:
                self.sequence = []
            else:
                first_element = sequence[0]
                if issubclass(type(first_element), TransformStepBase):
                    self.sequence = sequence
                elif type(first_element) is dict:
                    self.sequence = self.build_from_metadata(sequence)
                else:
                    raise ValueError('Sequence not recognized. Must be list of TransformStepInterface or List of dict,'
                                     'received list of type {}'.format(type(first_element)))
        else:
            raise ValueError('Sequence must be a list, received type {}'.format(type(sequence)))

    def append(self, transform_step: TransformStepBase):
        self.sequence.append(transform_step)

    def rerun(self, data: Any) -> Any:
        live_steps = [step for step in self.sequence if type(step) == LiveTransformStep]
        if len(live_steps) == 0:
            raise RuntimeError('This TransformSequence contains no re-runable steps.')

        transformed_data = deepcopy(data)
        for step in live_steps:
            if type(step) == LiveTransformStep:
                transformed_data = step.rerun(transformed_data)
        return transformed_data

    def is_not_empty(self) -> bool:
        return len(self.sequence) > 0

    def get_info(self):
        info_list = []
        for step in self.sequence:
            info_list.append(step.get_info())
        return info_list

    @classmethod
    def build_from_metadata(cls, metadata: List[Dict]) -> List[TransformStepBase]:
        sequence = []
        for entry in metadata:
            step = TransformStepInterface.load(**entry)
            sequence.append(step)
        return sequence

    def print_steps(self):
        for i, step in enumerate(self.sequence):
            step.print_step(i)


class SelfAwareData:
    """A wrapper around a dataset that also contains the code that generated the data.
     `SelfAwareData` can re-run it's transformation steps on a new dataset."""

    def __init__(self, data: Any, metadata: TransformSequenceConvertible = None):
        self.data = data
        self.transform_sequence = TransformSequence(metadata)

    @classmethod
    def load_from_file(cls, file_path: str) -> 'SelfAwareData':
        """
        Create a SelfAwareData object with a initial FileSourceTransformStep

        Args:
            file_path: path to a standard file (not already a SelfAwareData)

        Returns: SelfAwareData with a TransformSequence containing a FileSourceTransformStep pointing to file_path

        """
        data_interface = DataInterfaceManager.select(file_path)
        data = data_interface.load(file_path)
        metadata = [{'file_path': file_path}]
        return cls(data, metadata)

    def get_info(self) -> List[Dict]:
        return self.transform_sequence.get_info()

    def transform(self, transformer_func: Callable, tag: str = '', enforce_clean_git=True,
                  get_git_hash_from: Any = None, **kwargs) -> 'SelfAwareData':
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
        transformed_data, transform_step = TransformStepInterface.execute(self.data, transformer_func, tag,
                                                                          enforce_clean_git=enforce_clean_git,
                                                                          get_git_hash_from=get_git_hash_from, **kwargs)
        new_sad = self._copy_and_extend(transformed_data, transform_step)
        return new_sad

    def _copy_and_extend(self, transformed_data, transform_step) -> 'SelfAwareData':
        """Generate a new SAD object from the existing one, adding an additional step"""
        new_sad = SelfAwareData(transformed_data, deepcopy(self.transform_sequence))
        new_sad.transform_sequence.append(transform_step)
        return new_sad

    def rerun(self, data) -> Any:
        """
        Rerun the same transformation function that generated this `SelfAwareData` on a new data object.

        Args:
            data:

        Returns:

        """
        return self.transform_sequence.rerun(data)

    def print_steps(self):
        """Print the code of the transformation steps that generated the data."""
        self.transform_sequence.print_steps()

    def save(self, file_path: str,  **kwargs) -> Path:
        """
        Save a SelfAwareData object.

        Args:
            file_path: Path for where to save the SAD, including file extension.

        Returns: Path to the saved SAD
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
            data_interface_hint: Hint for which kind of data interface to use to load the data (file extension).

        Example Usage:
            >>> sad = SelfAwareData.load('~/project/data/sad_dir__2021-01-01_12-00__standard_features.csv')
        """
        return SelfAwareDataInterface.load(file_path, data_interface_hint, **kwargs)


class VersionedSelfAwareDataInterface:

    version = None
    file_component_interfaces = {}

    @classmethod
    def save(cls, sad: SelfAwareData, parent_path: str, file_name: str,  **kwargs) -> Path:
        raise NotImplementedError

    @classmethod
    def load(cls, path: str, data_interface_hint=None, load_function: bool = True, **kwargs) -> 'SelfAwareData':
        raise NotImplementedError

    @classmethod
    def get_info(cls, path: str) -> Dict:
        raise NotImplementedError

    @classmethod
    def get_printable_filename(cls, path: str) -> str:
        raise NotImplementedError

    @classmethod
    def get_data_type(cls, path: str) -> str:
        raise NotImplementedError

    @classmethod
    def _generate_name_for_transform_dir(cls, git_hash: str, tag: str = None) -> str:
        raise NotImplementedError

    @classmethod
    def _parse_transform_dir_name(cls, path) -> Tuple[str, str, str]:
        raise NotImplementedError

    @classmethod
    def _identify_transform_sub_files(cls, path: str) -> Dict[str, str]:
        glob_path = Path(path, '*')
        subpaths = glob.glob(glob_path.__str__())
        file_map = {}
        for file_component in cls.file_component_interfaces:
            file_map[file_component] = cls._identify_sub_file(subpaths, file_component)
        return file_map

    @classmethod
    def _identify_sub_file(cls, file_contents: List[Path], key: str) -> str:
        options = [str(file_path) for file_path in file_contents if key in os.path.basename(file_path)]
        if len(options) == 0:
            raise ValueError('No {} file found for SelfAwareData'.format(key))
        elif len(options) > 1:
            raise ValueError('More than one {} file found for SelfAwareData: {}'.format(key, ', '.join(options)))
        else:
            return options[0]


class SelfAwareDataInterface_v0(VersionedSelfAwareDataInterface):
    """DataInterface for saving and loading `SelfAwareData` objects."""

    version = 0
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
        info = sad.get_info()
        if len(info) > 0:
            git_hash = info[-1].get('git_hash', '')
            code = info[-1].get('code', '')
            latest_transform_step = sad.transform_sequence.sequence[-1]
            if type(latest_transform_step) == LiveTransformStep:
                transformer_func = latest_transform_step.transformer_func
            else:
                transformer_func = None
        else:
            git_hash = ''
            transformer_func = None
            code = ''
        transform_dir_name = cls._generate_name_for_transform_dir(git_hash, tag)
        new_transform_dir_path = Path(parent_path, transform_dir_name)
        os.makedirs(new_transform_dir_path)

        data_interface = DataInterfaceManager.select(data_file_type)
        data_interface.save(sad.data, 'data', new_transform_dir_path, **kwargs)
        cls.file_component_interfaces['func'].save(transformer_func, 'func', new_transform_dir_path)
        cls.file_component_interfaces['code'].save(code, 'code', new_transform_dir_path)

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

        metadata = {'code': transformer_code, **info}
        transform_step = LiveTransformStep(metadata=metadata, transformer_func=transformer_func)
        transform_sequence = TransformSequence([transform_step])
        return SelfAwareData(data, transform_sequence)

    @classmethod
    def get_info(cls, path: str) -> Dict[str, str]:
        timestamp, git_hash, tag = cls._parse_transform_dir_name(path)
        file_map = cls._identify_transform_sub_files(path)
        data_file_root, data_file_type = os.path.splitext(file_map['data'])
        data_file_type = data_file_type.replace('.', '')
        return {
            'timestamp': SADTimestamp.format(timestamp),
            'git_hash': git_hash,
            'tag': tag,
            'data_type': data_file_type
        }

    @classmethod
    def get_printable_filename(cls, path) -> str:
        """Build the filename that should be printed to describe the TransformedDataDirectory.
         The filename is created based on the TransformedDataDirectory tag and file type."""
        info = cls.get_info(path)
        effective_filename = '{}.{}'.format(info['tag'], info['data_type'])
        return effective_filename

    @classmethod
    def get_data_type(cls, path: str) -> str:
        return cls.get_info(path)['data_type']

    @classmethod
    def _generate_name_for_transform_dir(cls, git_hash: str, tag: str = None) -> str:
        timestamp = SADTimestamp.now()
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


class SelfAwareDataInterface_v1(VersionedSelfAwareDataInterface):
    """DataInterface for saving and loading `SelfAwareData` objects."""

    version = 1
    file_component_interfaces = {
        'data': None,
        'sad': DillDataInterface,
        'provenance': YAMLDataInterface,
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
        transform_dir_name = cls._generate_name_for_transform_dir(tag)
        new_transform_dir_path = Path(parent_path, transform_dir_name)
        os.makedirs(new_transform_dir_path)

        data_interface = DataInterfaceManager.select(data_file_type)
        data_interface.save(sad.data, 'data', new_transform_dir_path, **kwargs)

        cls.file_component_interfaces['sad'].save(sad, 'sad', new_transform_dir_path)

        provenance = {
            'interface_version': cls.version,
            'transform_steps': sad.get_info()
        }
        cls.file_component_interfaces['provenance'].save(provenance, 'provenance', new_transform_dir_path)

        print('created new SAD directory {}'.format(new_transform_dir_path))
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
        sad_file = file_map['sad']
        metadata_file = file_map['provenance']

        if load_function:
            sad = cls.file_component_interfaces['sad'].load(sad_file)
            return sad
        else:
            data_interface = DataInterfaceManager.select(data_file, default_file_type=data_interface_hint)
            data = data_interface.load(data_file, **kwargs)
            metadata = cls.file_component_interfaces['provenance'].load(metadata_file)
            # TODO: make function to add and extract version from sequence file
            metadata = metadata['sequence']
            return SelfAwareData(data, metadata)

    @classmethod
    def get_info(cls, path: str) -> Dict:
        timestamp, tag = cls._parse_transform_dir_name(path)
        file_map = cls._identify_transform_sub_files(path)
        metadata_file = file_map['provenance']
        metadata = cls.file_component_interfaces['provenance'].load(metadata_file)
        metadata['tag'] = tag
        metadata['timestamp'] = SADTimestamp.format(timestamp)
        metadata['data_type'] = cls.get_data_type(path)
        return metadata

    @classmethod
    def get_printable_filename(cls, path) -> str:
        """Build the filename that should be printed to describe the TransformedDataDirectory.
         The filename is created based on the TransformedDataDirectory tag and file type."""
        timestamp, tag = cls._parse_transform_dir_name(path)
        effective_filename = '{}.{}'.format(tag, cls.get_data_type(path))
        return effective_filename

    @classmethod
    def get_data_type(cls, path: str) -> str:
        file_map = cls._identify_transform_sub_files(path)
        data_file_root, data_file_type = os.path.splitext(file_map['data'])
        return data_file_type.replace('.', '')

    @classmethod
    def _generate_name_for_transform_dir(cls, tag: str = None) -> str:
        timestamp = SADTimestamp.now()
        delimiter_char = '__'
        file_name_components = ['sad_dir', timestamp]
        if tag is not None:
            file_name_components.append(tag)
        return delimiter_char.join(file_name_components)

    @classmethod
    def _parse_transform_dir_name(cls, path) -> Tuple[str, str]:
        delimiter_char = '__'
        dir_name = os.path.basename(path)
        dir_name_components = dir_name.split(delimiter_char)
        if len(dir_name_components) == 2:
            denoter, timestamp = dir_name_components
            tag = ''
        elif len(dir_name_components) == 3:
            denoter, timestamp, tag = dir_name_components
        else:
            raise ValueError('SelfAwareDataDirectory name could not be parsed: {}'.format(dir_name))
        return timestamp, tag


class SADInterfaceVersionManagerBase:

    def __init__(self):
        self.version_map = {}
        self.latest_version = None

    def register(self, interface: Type[VersionedSelfAwareDataInterface]):
        version = interface.version
        if version in self.version_map.keys():
            raise ValueError('There is already an interface registered as version {}'.format(version))
        self.version_map[version] = interface
        self._update_latest_version(version)

    def select_version_for_path(self, file_path: str) -> VersionedSelfAwareDataInterface:
        version = self.read_version_from_metadata_file(file_path)
        if version not in self.version_map:
            known_versions = list(self.version_map.keys())
            raise RuntimeError('SAD Interface version not recognized: {}. Known versions: {}'.format(version,
                                                                                                     known_versions))
        return self.version_map[version]

    def get_version(self, version):
        if version not in self.version_map.keys():
            raise ValueError('Interface version {} is not recognized'.format(version))
        return self.version_map[version]

    @property
    def latest(self) -> VersionedSelfAwareDataInterface:
        return self.version_map[self.latest_version]

    def _update_latest_version(self, version: int):
        if self.latest_version is None:
            self.latest_version = version
        elif version > self.latest_version:
            self.latest_version = version

    @classmethod
    def read_version_from_metadata_file(cls, path: str) -> int:
        metadata_path = Path(path, 'provenance.yaml')
        if metadata_path.is_file():
            metadata = YAMLDataInterface.load(metadata_path)
            if 'interface_version' not in metadata:
                raise RuntimeError('The SAD interface version could not be determined for {}'.format(path))
            version = metadata['interface_version']
        else:
            version = 0
        return version


SADInterfaceVersionManager = SADInterfaceVersionManagerBase()
SADInterfaceVersionManager.register(SelfAwareDataInterface_v0)
SADInterfaceVersionManager.register(SelfAwareDataInterface_v1)


class SelfAwareDataInterface:

    @classmethod
    def save(cls, sad: SelfAwareData, parent_path: str, file_name: str, interface_version=None,  **kwargs) -> Path:
        if interface_version is None:
            interface = SADInterfaceVersionManager.latest
        else:
            interface = SADInterfaceVersionManager.get_version(interface_version)
        return interface.save(sad, parent_path, file_name, **kwargs)

    @classmethod
    def load(cls, path: str, data_interface_hint=None, load_function: bool = True, **kwargs) -> 'SelfAwareData':
        versioned_interface = SADInterfaceVersionManager.select_version_for_path(path)
        return versioned_interface.load(path, data_interface_hint, load_function, **kwargs)

    @classmethod
    def get_info(cls, path: str) -> Dict:
        versioned_interface = SADInterfaceVersionManager.select_version_for_path(path)
        return versioned_interface.get_info(path)

    @classmethod
    def get_printable_filename(cls, path) -> str:
        versioned_interface = SADInterfaceVersionManager.select_version_for_path(path)
        return versioned_interface.get_printable_filename(path)

    @classmethod
    def get_data_type(cls, path):
        versioned_interface = SADInterfaceVersionManager.select_version_for_path(path)
        return versioned_interface.get_data_type(path)
