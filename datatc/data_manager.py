import warnings
from .data_directory import DataDirectory, DataDirectoryManager
from typing import Type

CONFIG_FILE_NAME = '.data_map.yaml'


class DataManager:
    """
    Keep track of project data directories.

    """

    data_dir_manager = DataDirectoryManager

    def __init__(self, path_hint: str, data_dir_manager: Type[DataDirectoryManager] = DataDirectoryManager):
        """
        Initialize a DataManager pointing at a project's data_path.

        Args:
            path_hint: the name of a project that has been previously registered to `DataManager`, or a path to a data
                directory.
            data_dir_manager: DataDirectoryManager to use to interact with registered DataDirectories
        """
        self.data_path = data_dir_manager.load_project_path_from_hint(path_hint)
        self.data_directory = DataDirectory(self.data_path.__str__())
        warnings.warn('DataManager is deprecated. Please use `DataDirectory.load()` instead.', DeprecationWarning,
                      stacklevel=2)

    def reload(self):
        """Refresh the data directory contents that `DataManager` is aware of.
        Useful if you have created a new file on the file system without using `DataManager`, and now need `DataManager`
        to know about it. """
        self.data_directory = DataDirectory(self.data_path)

    def __getitem__(self, key):
        return self.data_directory[key]

    @classmethod
    def register_project(cls, project_hint: str, project_path: str,
                         data_dir_manager: Type[DataDirectoryManager] = DataDirectoryManager) -> None:
        return data_dir_manager.register_project(project_hint, project_path)

    @classmethod
    def list_projects(cls, data_dir_manager: Type[DataDirectoryManager] = DataDirectoryManager) -> None:
        return data_dir_manager.list_projects()

    def ls(self, full: bool = False) -> None:
        """
        List the contents of the data directory.

        Args:
            full: If True, prints the full data directory contents. If false, prints only a summary of the file types
             contained in each directory (prints all subdirectories).

        """
        self.data_directory.ls(full=full)
