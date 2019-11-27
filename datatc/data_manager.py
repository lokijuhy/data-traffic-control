import datetime
import glob
import inspect
import os
from pathlib import Path
from typing import Any, Callable
import yaml

import datatc.data_interface as di
import datatc.git_utilities as gu


class DataManager:
    """
    Manage the storage, retrieval, versioning, and provenance of data in various formats.

    """

    def __init__(self, path_hint):
        """
        Initialize a DataManager pointing at a project data_path. Can refer to ~/.data_manager.yaml, which has format:
            project:
                path: ~/path/to/project
        Args:
            path_hint: a path that exists, or a key in the config for a path

       Note:
           On windows OS, the backslash in the string should be escaped!!
        """
        self.data_path = self._identify_data_path(path_hint)
        self.data = {}
        self.DataProcessorCacheManager = DataProcessorCacheManager()

    def _identify_data_path(self, path_hint):
        """
        Determine the data_path from the path_hint.
        The path_hint may be a ligitimate path, in which case use it.
        Otherwise, look for a DataManager config, and look for path_hint within the config.
        If neither of the above work, raise an error.

        Args:
            path_hint: str.

        Returns:

        """
        expanded_path = Path(path_hint).expanduser()
        if expanded_path.exists():
            return expanded_path

        config = self._load_config()
        if config is not None and path_hint in config:
            expanded_config_path = Path(config[path_hint]['path']).expanduser()
            if expanded_config_path.exists():
                return expanded_config_path
            else:
                raise ValueError("Path provided in config for '{}' does not exist: {}".format(path_hint,
                                                                                              expanded_config_path))

        raise ValueError("Path does not exist: {}".format(path_hint))

    @staticmethod
    def _load_config():
        config_path = Path('~/.data_manager.yaml').expanduser()
        if config_path.exists():
            config = yaml.safe_load(open(config_path))
            return config
        else:
            return None

    @staticmethod
    def get_repo_path():
        """
        Get the path to the parent dir of the git repo containing this file.
        TODO: how get the git directory of the code directory that is calling/subclassing DataManager?

        Returns: str.

        """
        return os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

    @classmethod
    def check_for_uncommitted_git_changes(cls):
        repo_path = cls.get_repo_path()
        return gu.check_for_uncommitted_git_changes_at_path(repo_path)

    @classmethod
    def generate_filename_for_file(cls, tag: str = None) -> str:
        repo_path = cls.get_repo_path()
        git_hash = gu.get_git_hash(repo_path)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        if tag:
            filename = "{}_{}_{}".format(timestamp, git_hash, tag)
        else:
            filename = "{}_{}".format(timestamp, git_hash)
        return filename

    def magic_load(self, sub_path: str, filename: str) -> Any:
        """
        Magically load any data file anywhere in the project
        Args:
            sub_path: path between self.data_path and the file
            filename: name of file to load, with file extention.

        Returns: data!

        """
        # TODO write magic_load!
        return

    def cache_data_processor(self, data: Any, processing_func: Callable, tag: str = None, data_file_type='pkl',
                             enforce_clean_git=True) -> str:
        """
        Run and save the results of a data processing step. Saves the processing logic alongside the output, so that
        the data can be reproduced.

        Args:
            data: input data for processing_func
            processing_func: The data transformation function to apply and save the results of
            tag: short description to put in the file names
            data_file_type: File type to use for the saved data
            enforce_clean_git: Whether to only allow execution if the git state of the repo is clean.

        Returns: File name of saved data file.

        Raises: RuntimeError if git repo is in an unclean state.

        """
        if enforce_clean_git:
            self.check_for_uncommitted_git_changes()

        file_name = self.generate_filename_for_file(tag)
        file_data_path = Path(self.data_path, "data/transformed_data")
        self.DataProcessorCacheManager.save(data, processing_func, file_name=file_name, data_file_type=data_file_type,
                                            file_dir_path=file_data_path)
        return file_name

    def load_cached_data_processor(self, file_name: str) -> 'DataProcessor':
        file_data_path = Path(self.data_path, "data/transformed_data")
        return self.DataProcessorCacheManager.load(file_name=file_name, file_dir_path=file_data_path)

    def load_most_recent(self, sub_path):
        """Load the most recent pickled data dictionary from the data/transformed directory,
        as determined by the timestamp in the filename. """

        filepath = Path(self.data_path, '{}/*'.format(sub_path))
        files = glob.glob(filepath.__str__())

        if len(files) == 0:
            # TODO: make this raise some kind of error
            print("ERROR: no files found at {}".format(filepath))
            return

        filenames = [os.path.basename(file) for file in files]
        filenames.sort()
        chosen_one = filenames[-1]
        print("Loading {}".format(chosen_one))
        return self.magic_load(sub_path, chosen_one)


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
