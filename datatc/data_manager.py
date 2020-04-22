from pathlib import Path
from typing import Dict
import yaml

from datatc import data_processor
from datatc.data_directory import DataDirectory

CONFIG_FILE_NAME = '.data_map.yaml'


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
        self.data_directory = DataDirectory(self.data_path.__str__())
        self.DataProcessorCacheManager = data_processor.DataProcessorCacheManager()

    def reload(self):
        self.data_directory = DataDirectory(self.data_path)

    def __getitem__(self, key):
        return self.data_directory[key]

    @classmethod
    def register_project(cls, project_hint: str, project_path: str) -> None:
        """
        Register project and its data path to the config.
        If no config exists, create one.

        Args:
            project_hint: Name for project
            project_path: Path to project's data directory

        Returns: None.

        Raises: ValueError if project_hint already exists in file

        """
        # check that project_path is a valid path
        expanded_project_path = Path(project_path).expanduser()
        if not expanded_project_path.exists():
            raise FileNotFoundError("Not a valid path: '{}'".format(project_path))

        config_file_path = Path(Path.home(), CONFIG_FILE_NAME)
        if not config_file_path.exists():
            cls._init_config()

        config = cls._load_config()
        hint_already_in_file = cls._check_for_entry_in_config(project_hint, config)
        if hint_already_in_file:
            raise ValueError("Project hint '{}' is already registered".format(project_hint))

        cls._register_project_to_file(project_hint, expanded_project_path, config_file_path)

    @classmethod
    def list_projects(cls) -> None:
        """
        List the projects registered to the config.

        Returns: None. Just prints.

        """
        config = cls._load_config()
        if len(config) == 0:
            print("No projects registered!")
        for project_hint in config:
            print("{}: {}".format(project_hint, config[project_hint]['path']))

    @staticmethod
    def _init_config():
        """Create an empty config file."""
        config_path = Path(Path.home(), CONFIG_FILE_NAME)
        print("Creating config at {}".format(config_path))
        open(config_path.__str__(), 'x').close()

    @staticmethod
    def _config_exists() -> bool:
        """Determine whether a config file exists"""
        config_path = Path(Path.home(), CONFIG_FILE_NAME)
        if config_path.exists():
            return True
        else:
            return False

    def _load_config(self) -> Dict:
        """Load the config file. If config file is empty, return an empty dict."""
        if self._config_exists():
            config_path = Path(Path.home(), CONFIG_FILE_NAME)
            config = yaml.safe_load(open(config_path.__str__()))
            if config is None:
                config = {}
            return config
        else:
            raise FileNotFoundError('Config file not found at: {}'.format(config_path))

    @staticmethod
    def _check_for_entry_in_config(project_hint: str, config: Dict) -> bool:
        """
        Returns whether project_hint already exists in config file.

        Args:
            project_hint: Name for the project.
            config: The config dict.

        Returns: Bool for whether the project_hint is registered in the config.

        """
        if config is None:
            return False

        if project_hint in config:
            return True
        else:
            return False

    @classmethod
    def _get_path_for_project_hint(cls, project_hint: str, config: Dict) -> Path:
        if cls._check_for_entry_in_config(project_hint, config):
            return Path(config[project_hint]['path'])
        else:
            raise ValueError("Project hint '{}' is not registered".format(project_hint))

    @staticmethod
    def _register_project_to_file(project_hint: str, project_path: Path, config_file_path: Path):
        """
        Appends project details to specified config file.

        Args:
            project_hint: The name for the project.
            project_path: Path to project data directory.
            config_file_path: Path to config file.

        Returns: None.

        """
        config_entry_data = {
            project_hint: {
                'path': project_path.__str__(),
            }
        }
        with open(config_file_path.__str__(), 'a') as f:
            yaml.dump(config_entry_data, f, default_flow_style=False)

    def ls(self, full: bool = False):
        self.data_directory.ls(full=full)

    def _identify_data_path(self, path_hint):
        """
        Determine the data_path from the path_hint.
          Look for a DataManager config, and look for path_hint within the config.
          Otherwise, the path_hint may be a legitimate path, in which case use it.
          If neither of the above work, raise an error.

        Args:
            path_hint: str.

        Returns:

        """

        if self._config_exists():
            config = self._load_config()
            if config is not None and path_hint in config:
                expanded_config_path = Path(config[path_hint]['path']).expanduser()
                if expanded_config_path.exists():
                    return expanded_config_path
                else:
                    raise ValueError("Path provided in config for '{}' does not exist: {}".format(path_hint,
                                                                                                  expanded_config_path))

        expanded_path = Path(path_hint).expanduser()
        if expanded_path.exists():
            return expanded_path

        raise ValueError("Provided hint '{}' is not registered and is not a valid path. "
                         "\n\nRegister your project with `DataManager.register_project(project_hint, project_path)`"
                         "".format(path_hint))

    # --- everything below this line probably belongs in some other class --------------------------------------------

    # @staticmethod
    # def get_repo_path():
    #     """
    #     Get the path to the parent dir of the git repo containing this file.
    #     TODO: how get the git directory of the code directory that is calling/subclassing DataManager?
    #
    #     Returns: str.
    #
    #     """
    #     return os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    #
    # @classmethod
    # def check_for_uncommitted_git_changes(cls):
    #     repo_path = cls.get_repo_path()
    #     return gu.check_for_uncommitted_git_changes_at_path(repo_path)
    #
    # @classmethod
    # def generate_filename_for_file(cls, tag: str = None) -> str:
    #     repo_path = cls.get_repo_path()
    #     git_hash = gu.get_git_hash(repo_path)
    #     timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    #     if tag:
    #         filename = "{}_{}_{}".format(timestamp, git_hash, tag)
    #     else:
    #         filename = "{}_{}".format(timestamp, git_hash)
    #     return filename
    #
    # def cache_data_processor(self, data: Any, processing_func: Callable, tag: str = None, data_file_type='pkl',
    #                          enforce_clean_git=True) -> str:
    #     """
    #     Run and save the results of a data processing step. Saves the processing logic alongside the output, so that
    #     the data can be reproduced.
    #
    #     Args:
    #         data: input data for processing_func
    #         processing_func: The data transformation function to apply and save the results of
    #         tag: short description to put in the file names
    #         data_file_type: File type to use for the saved data
    #         enforce_clean_git: Whether to only allow execution if the git state of the repo is clean.
    #
    #     Returns: File name of saved data file.
    #
    #     Raises: RuntimeError if git repo is in an unclean state.
    #
    #     """
    #     if enforce_clean_git:
    #         self.check_for_uncommitted_git_changes()
    #
    #     file_name = self.generate_filename_for_file(tag)
    #     file_data_path = Path(self.data_path, "data/transformed_data")
    #     self.DataProcessorCacheManager.save(data, processing_func, file_name=file_name, data_file_type=data_file_type,
    #                                         file_dir_path=file_data_path)
    #     return file_name
    #
    # def load_cached_data_processor(self, file_name: str) -> 'DataProcessor':
    #     file_data_path = Path(self.data_path, "data/transformed_data")
    #     return self.DataProcessorCacheManager.load(file_name=file_name, file_dir_path=file_data_path)
    #
    # def load_most_recent(self, sub_path):
    #     """Load the most recent pickled data dictionary from the data/transformed directory,
    #     as determined by the timestamp in the filename. """
    #
    #     filepath = Path(self.data_path, '{}/*'.format(sub_path))
    #     files = glob.glob(filepath.__str__())
    #
    #     if len(files) == 0:
    #         # TODO: make this raise some kind of error
    #         print("ERROR: no files found at {}".format(filepath))
    #         return
    #
    #     filenames = [os.path.basename(file) for file in files]
    #     filenames.sort()
    #     chosen_one = filenames[-1]
    #     print("Loading {}".format(chosen_one))
    #     return self.magic_load(sub_path, chosen_one)
    #
    #
