from pathlib import Path
from typing import Dict
import yaml

from datatc.data_directory import DataDirectory

CONFIG_FILE_NAME = '.data_map.yaml'


class DataManager:
    """
    Keep track of project data directories.

    """

    def __init__(self, path_hint: str):
        """
        Initialize a DataManager pointing at a project's data_path.

        Args:
            path_hint: the name of a project that has been previously registered to `DataManager`, or a path to a data
                directory.

        """
        self.data_path = self._identify_data_path(path_hint)
        self.data_directory = DataDirectory(self.data_path.__str__())

    def reload(self):
        """Refresh the data directory contents that `DataManager` is aware of.
        Useful if you have created a new file on the file system without using `DataManager`, and now need `DataManager`
        to know about it. """
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
        """List the projects known to `DataManager`."""
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

    @classmethod
    def _load_config(cls) -> Dict:
        """Load the config file. If config file is empty, return an empty dict."""
        config_path = Path(Path.home(), CONFIG_FILE_NAME)
        if cls._config_exists():
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

    def ls(self, full: bool = False) -> None:
        """
        List the contents of the data directory.

        Args:
            full: If True, prints the full data directory contents. If false, prints only a summary of the file types
             contained in each directory (prints all subdirectories).

        """
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
